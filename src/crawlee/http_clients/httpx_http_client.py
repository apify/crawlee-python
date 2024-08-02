from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, cast

import httpx
from typing_extensions import override

from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee.errors import HttpStatusCodeError, ProxyError
from crawlee.http_clients import BaseHttpClient, HttpCrawlingResult, HttpResponse
from crawlee.sessions import Session

if TYPE_CHECKING:
    from collections.abc import Iterable

    from crawlee.models import Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.statistics import Statistics
    from crawlee.types import HttpMethod


class _HttpxResponse:
    """Adapter class for `httpx.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response

    def read(self) -> bytes:
        """Read the content of the response body."""
        return self._response.read()

    @property
    def status_code(self) -> int:
        """HTTP status code of the response."""
        return self._response.status_code

    @property
    def headers(self) -> dict[str, str]:
        """HTTP headers of the response."""
        return dict(self._response.headers.items())


class _HttpxTransport(httpx.AsyncHTTPTransport):
    """A modified HTTP transport adapter that stores response cookies in a `Session` instead of the `HTTPX` client."""

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


class HttpxHttpClient(BaseHttpClient):
    """A `HTTPX` based HTTP client used for making HTTP calls in crawlers (`BasicCrawler` subclasses)."""

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **async_client_kwargs: Any,
    ) -> None:
        """Create a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors.
            ignore_http_error_status_codes: HTTP status codes to ignore as errors.
            async_client_kwargs: Additional keyword arguments for `httpx.AsyncClient`.
        """
        self._persist_cookies_per_session = persist_cookies_per_session
        self._additional_http_error_status_codes = set(additional_http_error_status_codes)
        self._ignore_http_error_status_codes = set(ignore_http_error_status_codes)
        self._async_client_kwargs = async_client_kwargs

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

        http_request = client.build_request(
            method=request.method,
            url=request.url,
            headers=request.headers,
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

        exclude_error = response.status_code in self._ignore_http_error_status_codes
        include_error = response.status_code in self._additional_http_error_status_codes

        if include_error or (self._is_server_code(response.status_code) and not exclude_error):
            if include_error:
                raise HttpStatusCodeError(
                    f'Status code {response.status_code} (user-configured to be an error) returned'
                )

            raise HttpStatusCodeError(f'Status code {response.status_code} returned')

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
        headers: dict[str, str] | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        client = self._get_client(proxy_info.url if proxy_info else None)

        http_request = client.build_request(
            url=url,
            method=method,
            headers=headers,
            extensions={'crawlee_session': session if self._persist_cookies_per_session else None},
        )

        try:
            response = await client.send(http_request)
        except httpx.TransportError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        return _HttpxResponse(response)

    def _get_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        """Helper to get a HTTP client for the given proxy URL.

        If the client for the given proxy URL doesn't exist, it will be created and stored.
        """
        if proxy_url not in self._client_by_proxy_url:
            self._client_by_proxy_url[proxy_url] = httpx.AsyncClient(
                transport=_HttpxTransport(),
                proxy=proxy_url,
                timeout=httpx.Timeout(10),
                **self._async_client_kwargs,
            )

        return self._client_by_proxy_url[proxy_url]

    @staticmethod
    def _is_proxy_error(error: httpx.TransportError) -> bool:
        """Helper to check whether the given error is a proxy-related error."""
        if isinstance(error, httpx.ProxyError):
            return True

        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
            return True

        return False
