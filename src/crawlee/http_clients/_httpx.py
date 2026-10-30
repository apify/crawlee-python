from __future__ import annotations

import asyncio
import warnings
from functools import cached_property
from http.cookiejar import CookieJar, DefaultCookiePolicy
from logging import DEBUG, WARNING, getLogger
from typing import TYPE_CHECKING, Any

import httpx2
from typing_extensions import override

from crawlee._log_config import get_configured_log_level
from crawlee._types import HttpHeaders
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee._utils.http import drain_small_body
from crawlee.errors import ProxyError
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.http_clients import HttpClient, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import timedelta
    from ssl import SSLContext

    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session

logger = getLogger(__name__)


class _HttpxResponse(HttpResponse):
    """Adapter class for `httpx2.Response` to implement `HttpResponse`."""

    def __init__(self, response: httpx2.Response, *, deadline: float | None) -> None:
        self._response = response
        self._deadline = deadline
        self._streamed = False

    @property
    def url(self) -> str:
        return str(self._response.url)

    @property
    def http_version(self) -> str:
        return self._response.http_version

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @cached_property
    def headers(self) -> HttpHeaders:
        return HttpHeaders(dict(self._response.headers))

    @property
    def headers_set_cookie(self) -> list[str]:
        return self._response.headers.get_list('set-cookie')

    async def read(self) -> bytes:
        if self._streamed:
            raise RuntimeError('Stream is already consumed.')
        try:
            async with asyncio.timeout_at(self._deadline):
                return await self._response.aread()
        except httpx2.TimeoutException as exc:
            raise TimeoutError from exc

    async def read_stream(self) -> AsyncIterator[bytes]:
        if self._streamed or self._response.is_stream_consumed:
            raise RuntimeError('Stream is already consumed.')
        self._streamed = True
        chunks = self._response.aiter_bytes()
        while True:
            try:
                async with asyncio.timeout_at(self._deadline):
                    chunk = await anext(chunks)
            except StopAsyncIteration:
                return
            except httpx2.TimeoutException as exc:
                raise TimeoutError from exc
            yield chunk

    async def aclose(self) -> None:
        try:
            await drain_small_body(self)
        finally:
            await self._response.aclose()


@docs_group('HTTP clients')
class HttpxHttpClient(HttpClient):
    """HTTP client based on the `HTTPX2` library.

    This client uses the `HTTPX2` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
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
        follow_redirects: bool = True,
        **async_client_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            http1: Whether to enable HTTP/1.1 support.
            http2: Whether to enable HTTP/2 support.
            verify: SSL certificates used to verify the identity of requested hosts.
            header_generator: Header generator instance to use for generating browser-like headers.
            follow_redirects: Whether to follow HTTP redirects.
            async_client_kwargs: Additional keyword arguments for `httpx2.AsyncClient`. Some of them are ignored with a
                warning. The `mounts` and `transport` arguments would bypass the proxy of a `ProxyInfo`. Cookies are
                kept in sessions, so `cookies` is ignored. The `auth` and `params` arguments would reach every request
                of a redirect chain, so pass credentials in the `Authorization` header and the query in the URL instead.
                Redirects are followed by `send_request`, so `max_redirects` is ignored. The `proxy` argument covers
                only the requests made without a `ProxyInfo`, a `ProxyConfiguration` takes precedence over it. The
                `limits` argument applies per proxy, because every proxy gets a connection pool of its own.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
            follow_redirects=follow_redirects,
            headers=async_client_kwargs.pop('headers', None),
        )

        # `httpx2` logs one INFO line per request, which is too noisy for the default log level. Silence it down to
        # WARNING unless the user has explicitly opted into DEBUG.
        httpx_logger = getLogger('httpx2')
        httpx_logger.setLevel(DEBUG if get_configured_log_level() <= DEBUG else WARNING)

        self._http1 = http1
        self._http2 = http2

        # `httpx2.AsyncClient` turns a `proxy` into a mount of its own, so it is handed to the transport instead. It
        # covers the requests that carry no `ProxyInfo` of their own.
        self._proxy = async_client_kwargs.pop('proxy', None)

        for ignored_kwarg in ('mounts', 'transport', 'cookies', 'auth', 'params', 'max_redirects'):
            if async_client_kwargs.pop(ignored_kwarg, None) is not None:
                warnings.warn(
                    f'The `{ignored_kwarg}` argument of `HttpxHttpClient` is ignored.',
                    UserWarning,
                    stacklevel=2,
                )

        self._async_client_kwargs = async_client_kwargs
        self._header_generator = header_generator

        self._ssl_context = httpx2.create_ssl_context(verify=verify)

        self._client_by_proxy_url = dict[str | None, httpx2.AsyncClient]()

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
        # The headers are generated once per call, so that every request of a redirect chain carries the same browser
        # profile.
        if not isinstance(headers, HttpHeaders):
            headers = HttpHeaders(headers or {})

        return await super().send_request(
            url,
            method=method,
            headers=self._combine_headers(headers),
            payload=payload,
            session=session,
            proxy_info=proxy_info,
            timeout=timeout,
        )

    @override
    async def _fetch(
        self,
        url: str,
        *,
        method: HttpMethod,
        headers: HttpHeaders,
        payload: HttpPayload | None,
        proxy_info: ProxyInfo | None,
        timeout: timedelta | None,
    ) -> _HttpxResponse:
        client = self._get_client(proxy_info.url if proxy_info else None)
        deadline = asyncio.get_running_loop().time() + timeout.total_seconds() if timeout is not None else None

        http_request = client.build_request(
            url=url,
            method=method,
            headers=dict(headers) or None,
            content=payload,
            timeout=httpx2.Timeout(timeout.total_seconds()) if timeout is not None else httpx2.USE_CLIENT_DEFAULT,
        )

        try:
            response = await client.send(http_request, stream=True)
        except httpx2.TimeoutException as exc:
            raise TimeoutError from exc
        except httpx2.TransportError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        return _HttpxResponse(response, deadline=deadline)

    def _get_client(self, proxy_url: str | None) -> httpx2.AsyncClient:
        """Retrieve or create an HTTP client for the given proxy URL.

        If a client for the specified proxy URL does not exist, create and store a new one.
        """
        if proxy_url not in self._client_by_proxy_url:
            # A client built with `proxy=` mounts its own transport and never calls the one given to `transport=`,
            # so the proxy goes on the transport.
            transport = httpx2.AsyncHTTPTransport(
                http1=self._http1,
                http2=self._http2,
                verify=self._ssl_context,
                proxy=proxy_url or self._proxy,
                # Above the `httpx2` default of 20 kept-alive connections every request pays a TCP and TLS handshake.
                limits=self._async_client_kwargs.get(
                    'limits',
                    httpx2.Limits(max_connections=1000, max_keepalive_connections=200),
                ),
            )

            # Prepare a default kwargs for the new client.
            kwargs: dict[str, Any] = {
                'http1': self._http1,
                'http2': self._http2,
            }

            # Update the default kwargs with any additional user-provided kwargs.
            kwargs.update(self._async_client_kwargs)

            kwargs.update(
                {
                    'transport': transport,
                    'verify': self._ssl_context,
                    # Redirects are followed hop by hop by `send_request`, which also handles the cookies.
                    'follow_redirects': False,
                    # With both cookie protocols off, the jar of the client never stores a cookie.
                    'cookies': CookieJar(policy=DefaultCookiePolicy(netscape=False, rfc2965=False)),
                }
            )

            client = httpx2.AsyncClient(**kwargs)
            self._client_by_proxy_url[proxy_url] = client

        return self._client_by_proxy_url[proxy_url]

    def _combine_headers(self, explicit_headers: HttpHeaders) -> HttpHeaders:
        """Merge generated headers with the headers of the client and explicit headers for an HTTP request.

        The generated headers come from a single browser profile, so that the set stays consistent. The headers of the
        client win over the generated ones, and explicit headers win over both.
        """
        if self._header_generator:
            generated_headers = self._header_generator.get_specific_headers(
                header_names={'Accept', 'Accept-Language', 'User-Agent'},
            )
        else:
            generated_headers = HttpHeaders()

        return generated_headers | self._headers | explicit_headers

    @staticmethod
    def _is_proxy_error(error: httpx2.TransportError) -> bool:
        """Determine whether the given error is related to a proxy issue.

        Check if the error is an instance of `httpx2.ProxyError` or if its message contains known proxy-related
        error keywords.
        """
        if isinstance(error, httpx2.ProxyError):
            return True

        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
            return True

        return False

    async def cleanup(self) -> None:
        for client in self._client_by_proxy_url.values():
            await client.aclose()
        self._client_by_proxy_url.clear()
