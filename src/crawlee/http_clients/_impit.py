from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from http.cookiejar import CookieJar
from logging import getLogger
from typing import TYPE_CHECKING, Any, TypedDict

from cachetools import LRUCache
from impit import AsyncClient, Browser, HTTPError, Response, TimeoutException, TransportError
from impit import ProxyError as ImpitProxyError
from typing_extensions import override

from crawlee._types import HttpHeaders
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import validate_http_url
from crawlee.errors import ProxyError
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from datetime import timedelta

    from crawlee import Request
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics

logger = getLogger(__name__)

# Cache key: (proxy_url, id(cookie_jar) or None)
_ClientCacheKey = tuple[str | None, int | None]


class _ClientCacheEntry(TypedDict):
    """Type definition for client cache entries."""

    client: AsyncClient
    cookie_jar: CookieJar | None


class _ImpitResponse:
    """Adapter class for `impit.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: Response) -> None:
        self._response = response

    @property
    def http_version(self) -> str:
        return str(self._response.http_version)

    @property
    def status_code(self) -> int:
        return int(self._response.status_code)

    @property
    def headers(self) -> HttpHeaders:
        return HttpHeaders(dict(self._response.headers))

    async def read(self) -> bytes:
        if not self._response.is_closed:
            raise RuntimeError('Use `read_stream` to read the body of the Response received from the `stream` method')
        return self._response.content

    async def read_stream(self) -> AsyncIterator[bytes]:
        if self._response.is_stream_consumed:
            raise RuntimeError('Stream is already consumed.')
        else:
            async for chunk in self._response.aiter_bytes():
                yield chunk


@docs_group('HTTP clients')
class ImpitHttpClient(HttpClient):
    """HTTP client based on the `impit` library.

    This client uses the `impit` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `HttpClient` class for more common information about HTTP clients.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler  # or any other HTTP client-based crawler
    from crawlee.http_clients import ImpitHttpClient

    http_client = ImpitHttpClient()
    crawler = HttpCrawler(http_client=http_client)
    ```
    """

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        http3: bool = False,
        verify: bool = True,
        browser: Browser | None = 'firefox',
        **async_client_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            http3: Whether to enable HTTP/3 support.
            verify: SSL certificates used to verify the identity of requested hosts.
            browser: Browser to impersonate.
            async_client_kwargs: Additional keyword arguments for `impit.AsyncClient`.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
        )
        self._http3 = http3
        self._verify = verify
        self._browser = browser

        self._async_client_kwargs = async_client_kwargs

        self._client_cache = LRUCache[_ClientCacheKey, _ClientCacheEntry](maxsize=10)

    def _resolve_cookie_jar(self, session: Session | None) -> CookieJar | None:
        """Resolve the cookie jar to use for a request.

        When cookie persistence is enabled, Impit mutates the session jar in place (same as attaching the jar
        directly). When persistence is disabled, return a deep-copied jar so existing cookies are still sent
        outbound, but response `Set-Cookie` values do not update the session.
        """
        if session is None:
            return None

        if self._persist_cookies_per_session:
            return session.cookies.jar

        # Copy cookies so Impit can attach a jar for outbound Cookie headers without mutating the session.
        jar = CookieJar()
        for cookie in session.cookies.jar:
            jar.set_cookie(deepcopy(cookie))
        return jar

    @override
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
        timeout: timedelta | None = None,
    ) -> HttpCrawlingResult:
        client = self._get_client(proxy_info.url if proxy_info else None, self._resolve_cookie_jar(session))

        try:
            response = await client.request(
                url=request.url,
                method=request.method,
                content=request.payload,
                headers=dict(request.headers) if request.headers else None,
                timeout=timeout.total_seconds() if timeout else None,
            )
        except TimeoutException as exc:
            raise asyncio.TimeoutError from exc
        except (TransportError, HTTPError) as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        if statistics:
            statistics.register_status_code(response.status_code)

        request.loaded_url = str(response.url)

        return HttpCrawlingResult(http_response=_ImpitResponse(response))

    @override
    async def send_request(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        timeout: timedelta | None = None,
    ) -> HttpResponse:
        validate_http_url(url)

        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        client = self._get_client(proxy_info.url if proxy_info else None, self._resolve_cookie_jar(session))

        try:
            response = await client.request(
                method=method,
                url=url,
                content=payload,
                headers=dict(headers) if headers else None,
                timeout=timeout.total_seconds() if timeout else None,
            )
        except TimeoutException as exc:
            raise asyncio.TimeoutError from exc
        except (TransportError, HTTPError) as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        return _ImpitResponse(response)

    @asynccontextmanager
    @override
    async def stream(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[HttpResponse]:
        validate_http_url(url)

        client = self._get_client(proxy_info.url if proxy_info else None, self._resolve_cookie_jar(session))

        try:
            response = await client.request(
                method=method,
                url=url,
                content=payload,
                headers=dict(headers) if headers else None,
                timeout=timeout.total_seconds() if timeout else None,
                stream=True,
            )
        except TimeoutException as exc:
            raise asyncio.TimeoutError from exc

        try:
            yield _ImpitResponse(response)
        finally:
            response.close()

    @staticmethod
    def _make_cache_key(proxy_url: str | None, cookie_jar: CookieJar | None) -> _ClientCacheKey:
        return (proxy_url, id(cookie_jar) if cookie_jar is not None else None)

    async def _close_client(self, client: AsyncClient) -> None:
        # Impit exposes cleanup via the async context manager protocol.
        result = client.__aexit__(None, None, None)
        if hasattr(result, '__await__'):
            await result  # type: ignore[misc]

    def _get_client(self, proxy_url: str | None, cookie_jar: CookieJar | None) -> AsyncClient:
        """Retrieve or create an HTTP client for the given proxy URL and cookie jar.

        Clients are cached by `(proxy_url, cookie_jar identity)` so sessions with different jars do not share
        a client. When cookie persistence is disabled, each request uses a fresh jar copy and therefore a
        short-lived client that is not retained in the cache.
        """
        # Ephemeral jars (persist_cookies_per_session=False) must not pollute / thrash the LRU cache.
        cacheable = cookie_jar is None or self._persist_cookies_per_session
        cache_key = self._make_cache_key(proxy_url, cookie_jar) if cacheable else None

        if cache_key is not None:
            cached_data = self._client_cache.get(cache_key)
            if cached_data and cached_data['cookie_jar'] is cookie_jar:
                return cached_data['client']

        # Prepare a default kwargs for the new client.
        kwargs: dict[str, Any] = {
            'proxy': proxy_url,
            'http3': self._http3,
            'verify': self._verify,
            'follow_redirects': True,
            'browser': self._browser,
        }

        # Update the default kwargs with any additional user-provided kwargs.
        kwargs.update(self._async_client_kwargs)

        client = AsyncClient(**kwargs, cookie_jar=cookie_jar)

        if cache_key is not None:
            # Close the client being evicted when the LRU is full, to avoid leaking connections.
            if len(self._client_cache) >= self._client_cache.maxsize:
                _evicted_key, evicted_entry = next(iter(self._client_cache.items()))
                asyncio.get_running_loop().create_task(self._close_client(evicted_entry['client']))

            self._client_cache[cache_key] = _ClientCacheEntry(client=client, cookie_jar=cookie_jar)

        return client

    @staticmethod
    def _is_proxy_error(error: HTTPError) -> bool:
        """Determine whether the given error is related to a proxy issue.

        Check if the error message contains known proxy-related error keywords.
        """
        if isinstance(error, ImpitProxyError):
            return True

        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
            return True

        return False

    @override
    async def cleanup(self) -> None:
        """Clean up resources used by the HTTP client."""
        for entry in list(self._client_cache.values()):
            await self._close_client(entry['client'])
        self._client_cache.clear()
