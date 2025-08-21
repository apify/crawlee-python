from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, TypedDict

from cachetools import LRUCache
from impit import AsyncClient, Browser, HTTPError, Response, TransportError
from impit import ProxyError as ImpitProxyError
from typing_extensions import override

from crawlee._types import HttpHeaders
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee.errors import ProxyError
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from datetime import timedelta
    from http.cookiejar import CookieJar

    from crawlee import Request
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics

logger = getLogger(__name__)


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
        http3: bool = True,
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

        self._client_by_proxy_url = LRUCache[str | None, _ClientCacheEntry](maxsize=10)

    @override
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
    ) -> HttpCrawlingResult:
        client = self._get_client(proxy_info.url if proxy_info else None, session.cookies.jar if session else None)

        try:
            response = await client.request(
                url=request.url,
                method=request.method,
                content=request.payload,
                headers=dict(request.headers) if request.headers else None,
            )
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
    ) -> HttpResponse:
        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        client = self._get_client(proxy_info.url if proxy_info else None, session.cookies.jar if session else None)

        try:
            response = await client.request(
                method=method, url=url, content=payload, headers=dict(headers) if headers else None
            )
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
        client = self._get_client(proxy_info.url if proxy_info else None, session.cookies.jar if session else None)

        response = await client.request(
            method=method,
            url=url,
            content=payload,
            headers=dict(headers) if headers else None,
            timeout=timeout.total_seconds() if timeout else None,
            stream=True,
        )
        try:
            yield _ImpitResponse(response)
        finally:
            # TODO: https://github.com/apify/impit/issues/242
            # Quickly closing Response while reading the response body causes an error in the Rust generator in `impit`.
            # With a short sleep and sync closing, the error does not occur.
            # Replace with `response.aclose` when this is resolved in impit.
            await asyncio.sleep(0.01)
            response.close()

    def _get_client(self, proxy_url: str | None, cookie_jar: CookieJar | None) -> AsyncClient:
        """Retrieve or create an HTTP client for the given proxy URL.

        If a client for the specified proxy URL does not exist, create and store a new one.
        """
        cached_data = self._client_by_proxy_url.get(proxy_url)
        if cached_data:
            client = cached_data['client']
            client_cookie_jar = cached_data['cookie_jar']
            if client_cookie_jar is cookie_jar:
                # If the cookie jar matches, return the existing client.
                return client

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

        self._client_by_proxy_url[proxy_url] = _ClientCacheEntry(client=client, cookie_jar=cookie_jar)

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
        self._client_by_proxy_url.clear()
