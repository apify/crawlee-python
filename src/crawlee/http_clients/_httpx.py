from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from logging import DEBUG, WARNING, getLogger
from typing import TYPE_CHECKING, Any, cast

import httpx
from typing_extensions import override

from crawlee._log_config import get_configured_log_level
from crawlee._types import HttpHeaders
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import validate_http_url
from crawlee.errors import ProxyError
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from datetime import timedelta
    from ssl import SSLContext

    from crawlee import Request
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics

logger = getLogger(__name__)


class _HttpxResponse:
    """Adapter class for `httpx.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response

    @property
    def http_version(self) -> str:
        return self._response.http_version

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> HttpHeaders:
        return HttpHeaders(dict(self._response.headers))

    async def read(self) -> bytes:
        if not self._response.is_closed:
            raise RuntimeError('Use `read_stream` to read the body of the Response received from the `stream` method')
        return await self._response.aread()

    async def read_stream(self) -> AsyncIterator[bytes]:
        if self._response.is_stream_consumed:
            raise RuntimeError('Stream is already consumed.')
        else:
            async for chunk in self._response.aiter_bytes():
                yield chunk


def _same_origin(url: httpx.URL, other: httpx.URL) -> bool:
    """Check whether two URLs share an origin."""
    return url.scheme == other.scheme and url.host == other.host and url.port == other.port


class _HttpxTransport(httpx.AsyncHTTPTransport):
    """HTTP transport adapter that keeps cookies in a `Session` instead of in the `HTTPX` client.

    Response cookies are stored in the session and the `Cookie` header is rebuilt from it before every hop, so
    one client can be shared by all sessions. A `Cookie` header passed by the caller wins for as long as the
    redirect chain stays on its origin.
    """

    def __init__(self, *args: Any, persist_cookies_per_session: bool, **kwargs: Any) -> None:
        """Initialize a new instance. Extra arguments are passed to `httpx.AsyncHTTPTransport`."""
        self._persist_cookies_per_session = persist_cookies_per_session
        super().__init__(*args, **kwargs)

    @override
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        session = cast('Session | None', request.extensions.get('crawlee_session'))
        original_url, user_cookie = request.extensions.get('crawlee_caller_cookie', (None, None))

        # The transport owns the `Cookie` header. Anything already on the request came from the `httpx` jar,
        # which is scoped to no session and no origin, so it is always replaced or dropped.
        if original_url is not None and _same_origin(original_url, request.url):
            request.headers['cookie'] = user_cookie
        elif session and (cookies := session.cookies.get_cookie_string(str(request.url))):
            request.headers['cookie'] = cookies
        else:
            request.headers.pop('cookie', None)

        response = await super().handle_async_request(request)
        response.request = request

        if self._persist_cookies_per_session and session:
            session.cookies.store_cookies(list(response.cookies.jar))

        if 'Set-Cookie' in response.headers:
            del response.headers['Set-Cookie']

        return response


@docs_group('HTTP clients')
class HttpxHttpClient(HttpClient):
    """HTTP client based on the `HTTPX` library.

    This client uses the `HTTPX` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `HttpClient` class for more common information about HTTP clients.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler  # or any other HTTP client-based crawler
    from crawlee.http_clients import HttpxHttpClient

    http_client = HttpxHttpClient()
    crawler = HttpCrawler(http_client=http_client)
    ```
    """

    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        http1: bool = True,
        http2: bool = True,
        verify: str | bool | SSLContext = True,
        header_generator: HeaderGenerator | None = _DEFAULT_HEADER_GENERATOR,
        **async_client_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            http1: Whether to enable HTTP/1.1 support.
            http2: Whether to enable HTTP/2 support.
            verify: SSL certificates used to verify the identity of requested hosts.
            header_generator: Header generator instance to use for generating common headers.
            async_client_kwargs: Additional keyword arguments for `httpx.AsyncClient`. The `proxy` argument is
                ignored, proxies are configured through `ProxyConfiguration`. The `limits` argument applies per
                proxy, because every proxy gets a connection pool of its own.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
        )

        # `httpx` logs one INFO line per request, which is too noisy for the default log level. Silence it down to
        # WARNING unless the user has explicitly opted into DEBUG.
        httpx_logger = getLogger('httpx')
        httpx_logger.setLevel(DEBUG if get_configured_log_level() <= DEBUG else WARNING)

        self._http1 = http1
        self._http2 = http2

        # A `proxy=` kwarg would mount a transport of its own and bypass the cookie handling.
        async_client_kwargs.pop('proxy', None)

        self._async_client_kwargs = async_client_kwargs
        self._header_generator = header_generator

        self._ssl_context = httpx.create_ssl_context(verify=verify)

        self._client_by_proxy_url = dict[str | None, httpx.AsyncClient]()

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
        client = self._get_client(proxy_info.url if proxy_info else None)

        http_request = self._build_request(
            client=client,
            session=session,
            url=request.url,
            method=request.method,
            headers=request.headers,
            payload=request.payload,
            timeout=httpx.Timeout(timeout.total_seconds()) if timeout is not None else None,
        )

        try:
            response = await client.send(http_request)
        except httpx.TimeoutException as exc:
            raise asyncio.TimeoutError from exc
        except httpx.TransportError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        if statistics:
            statistics.register_status_code(response.status_code)

        request.loaded_url = str(response.url)

        return HttpCrawlingResult(
            http_response=_HttpxResponse(response),
        )

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

        client = self._get_client(proxy_info.url if proxy_info else None)

        http_request = self._build_request(
            client=client,
            url=url,
            method=method,
            headers=headers,
            payload=payload,
            session=session,
            timeout=httpx.Timeout(timeout.total_seconds()) if timeout is not None else None,
        )

        try:
            response = await client.send(http_request)
        except httpx.TimeoutException as exc:
            raise asyncio.TimeoutError from exc
        except httpx.TransportError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        return _HttpxResponse(response)

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

        client = self._get_client(proxy_info.url if proxy_info else None)

        http_request = self._build_request(
            client=client,
            url=url,
            method=method,
            headers=headers,
            payload=payload,
            session=session,
            timeout=httpx.Timeout(None, connect=timeout.total_seconds()) if timeout else None,
        )

        try:
            response = await client.send(http_request, stream=True)
        except httpx.TimeoutException as exc:
            raise asyncio.TimeoutError from exc

        try:
            yield _HttpxResponse(response)
        finally:
            await response.aclose()

    def _build_request(
        self,
        *,
        client: httpx.AsyncClient,
        url: str,
        method: HttpMethod,
        headers: HttpHeaders | dict[str, str] | None,
        payload: HttpPayload | None,
        session: Session | None = None,
        timeout: httpx.Timeout | None = None,
    ) -> httpx.Request:
        """Build an `httpx.Request` using the provided parameters."""
        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        headers = self._combine_headers(headers)

        extensions: dict[str, Any] = {'crawlee_session': session}

        # `httpx` drops the `Cookie` header on every redirect but keeps the extensions, so the header of the caller
        # travels there. An empty header is kept as well, it suppresses the session cookies while the chain stays
        # on the origin the header was meant for.
        if (caller_cookie := headers.get('cookie')) is not None:
            extensions['crawlee_caller_cookie'] = (httpx.URL(url), caller_cookie)

        return client.build_request(
            url=url,
            method=method,
            headers=dict(headers) if headers else None,
            content=payload,
            extensions=extensions,
            timeout=timeout or httpx.USE_CLIENT_DEFAULT,
        )

    def _get_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        """Retrieve or create an HTTP client for the given proxy URL.

        If a client for the specified proxy URL does not exist, create and store a new one.
        """
        if proxy_url not in self._client_by_proxy_url:
            # A client built with `proxy=` mounts a transport of its own for proxied URLs and never calls the one
            # passed in `transport=`, so the proxy has to be handled by the transport to keep the cookie handling.
            transport_kwargs: dict[str, Any] = {
                'http1': self._http1,
                'http2': self._http2,
                'verify': self._ssl_context,
                'proxy': proxy_url,
                'persist_cookies_per_session': self._persist_cookies_per_session,
            }

            # Every proxy gets a pool of its own, so the `httpx` limits are left at their defaults.
            if 'limits' in self._async_client_kwargs:
                transport_kwargs['limits'] = self._async_client_kwargs['limits']

            transport = _HttpxTransport(**transport_kwargs)

            # Prepare a default kwargs for the new client.
            kwargs: dict[str, Any] = {
                'http1': self._http1,
                'http2': self._http2,
                'follow_redirects': True,
            }

            # Update the default kwargs with any additional user-provided kwargs.
            kwargs.update(self._async_client_kwargs)

            kwargs.update(
                {
                    'transport': transport,
                    'verify': self._ssl_context,
                }
            )

            client = httpx.AsyncClient(**kwargs)
            self._client_by_proxy_url[proxy_url] = client

        return self._client_by_proxy_url[proxy_url]

    def _combine_headers(self, explicit_headers: HttpHeaders | None) -> HttpHeaders:
        """Merge generated headers with explicit headers for an HTTP request.

        The generated headers come from a single browser profile, so that the set stays consistent. Explicit
        headers win over the generated ones.
        """
        if self._header_generator:
            generated_headers = self._header_generator.get_specific_headers(
                header_names={'Accept', 'Accept-Language', 'User-Agent'},
            )
        else:
            generated_headers = HttpHeaders()

        explicit_headers = explicit_headers or HttpHeaders()
        return generated_headers | explicit_headers

    @staticmethod
    def _is_proxy_error(error: httpx.TransportError) -> bool:
        """Determine whether the given error is related to a proxy issue.

        Check if the error is an instance of `httpx.ProxyError` or if its message contains known proxy-related
        error keywords.
        """
        if isinstance(error, httpx.ProxyError):
            return True

        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
            return True

        return False

    async def cleanup(self) -> None:
        for client in self._client_by_proxy_url.values():
            await client.aclose()
        self._client_by_proxy_url.clear()
