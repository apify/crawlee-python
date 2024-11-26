from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any, Optional, cast

import httpx
from typing_extensions import override

from crawlee._types import HttpHeaders
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee.errors import ProxyError
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.http_clients import BaseHttpClient, HttpCrawlingResult, HttpResponse
from crawlee.sessions import Session

if TYPE_CHECKING:
    from collections.abc import Iterable

    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.base_storage_client._models import Request
    from crawlee.proxy_configuration import ProxyInfo
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

    def read(self) -> bytes:
        return self._response.read()


class _HttpxTransport(httpx.AsyncHTTPTransport):
    """HTTP transport adapter that stores response cookies in a `Session`.

    This transport adapter modifies the handling of HTTP requests to update the session cookies
    based on the response cookies, ensuring that the cookies are stored in the session object
    rather than the `HTTPX` client itself.
    """

    @override
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await super().handle_async_request(request)
        response.request = request

        if session := cast(Session, request.extensions.get('crawlee_session')):
            response_cookies = httpx.Cookies()
            response_cookies.extract_cookies(response)
            session.cookies.update(response_cookies)

        if 'Set-Cookie' in response.headers:
            del response.headers['Set-Cookie']

        return response


@docs_group('Classes')
class HttpxHttpClient(BaseHttpClient):
    """HTTP client based on the `HTTPX` library.

    This client uses the `HTTPX` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `BaseHttpClient` class for more common information about HTTP clients.

    ### Usage

    ```python
    from crawlee.http_clients import HttpxHttpClient
    from crawlee.http_crawler import HttpCrawler  # or any other HTTP client-based crawler

    http_client = HttpxHttpClient()
    crawler = HttpCrawler(http_client=http_client)
    ```
    """

    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        http1: bool = True,
        http2: bool = True,
        header_generator: HeaderGenerator | None = _DEFAULT_HEADER_GENERATOR,
        **async_client_kwargs: Any,
    ) -> None:
        """A default constructor.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors.
            ignore_http_error_status_codes: HTTP status codes to ignore as errors.
            http1: Whether to enable HTTP/1.1 support.
            http2: Whether to enable HTTP/2 support.
            header_generator: Header generator instance to use for generating common headers.
            async_client_kwargs: Additional keyword arguments for `httpx.AsyncClient`.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
        )
        self._http1 = http1
        self._http2 = http2
        self._async_client_kwargs = async_client_kwargs
        self._header_generator = header_generator

        self._client_by_proxy_url = dict[Optional[str], httpx.AsyncClient]()

    @override
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
    ) -> HttpCrawlingResult:
        client = self._get_client(proxy_info.url if proxy_info else None)
        headers = self._combine_headers(request.headers)

        http_request = client.build_request(
            url=request.url,
            method=request.method,
            headers=headers,
            content=request.payload,
            cookies=session.cookies if session else None,
            extensions={'crawlee_session': session if self._persist_cookies_per_session else None},
        )

        try:
            response = await client.send(http_request, follow_redirects=True)
        except httpx.TransportError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        if statistics:
            statistics.register_status_code(response.status_code)

        self._raise_for_error_status_code(
            response.status_code,
            self._additional_http_error_status_codes,
            self._ignore_http_error_status_codes,
        )

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
    ) -> HttpResponse:
        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        client = self._get_client(proxy_info.url if proxy_info else None)
        headers = self._combine_headers(headers)

        http_request = client.build_request(
            url=url,
            method=method,
            headers=dict(headers) if headers else None,
            content=payload,
            extensions={'crawlee_session': session if self._persist_cookies_per_session else None},
        )

        try:
            response = await client.send(http_request)
        except httpx.TransportError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        self._raise_for_error_status_code(
            response.status_code,
            self._additional_http_error_status_codes,
            self._ignore_http_error_status_codes,
        )

        return _HttpxResponse(response)

    def _get_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        """Helper to get a HTTP client for the given proxy URL.

        If the client for the given proxy URL doesn't exist, it will be created and stored.
        """
        if proxy_url not in self._client_by_proxy_url:
            # Prepare a default kwargs for the new client.
            kwargs: dict[str, Any] = {
                'transport': _HttpxTransport(
                    proxy=proxy_url,
                    http1=self._http1,
                    http2=self._http2,
                ),
                'proxy': proxy_url,
                'http1': self._http1,
                'http2': self._http2,
            }

            # Update the default kwargs with any additional user-provided kwargs.
            kwargs.update(self._async_client_kwargs)

            client = httpx.AsyncClient(**kwargs)
            self._client_by_proxy_url[proxy_url] = client

        return self._client_by_proxy_url[proxy_url]

    def _combine_headers(self, explicit_headers: HttpHeaders | None) -> HttpHeaders | None:
        """Helper to get the headers for a HTTP request."""
        common_headers = self._header_generator.get_common_headers() if self._header_generator else HttpHeaders()
        user_agent_header = (
            self._header_generator.get_random_user_agent_header() if self._header_generator else HttpHeaders()
        )
        explicit_headers = explicit_headers or HttpHeaders()
        headers = common_headers | user_agent_header | explicit_headers
        return headers if headers else None

    @staticmethod
    def _is_proxy_error(error: httpx.TransportError) -> bool:
        """Helper to check whether the given error is a proxy-related error."""
        if isinstance(error, httpx.ProxyError):
            return True

        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
            return True

        return False
