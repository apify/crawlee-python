from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

try:
    from curl_cffi.requests import AsyncSession
    from curl_cffi.requests.errors import RequestsError
except ImportError as exc:
    raise ImportError(
        "To import anything from this subpackage, you need to install the 'curl-impersonate' extra."
        "For example, if you use pip, run `pip install 'crawlee[curl-impersonate]'`.",
    ) from exc

from typing_extensions import override

from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee.errors import HttpStatusCodeError, ProxyError
from crawlee.http_clients import BaseHttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from collections.abc import Iterable

    from curl_cffi.requests import Response

    from crawlee.models import Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics
    from crawlee.types import HttpHeaders, HttpMethod


class _CurlImpersonateResponse:
    """Adapter class for `curl_cffi.requests.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: Response) -> None:
        self._response = response

    def read(self) -> bytes:
        return self._response.content

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> dict[str, str]:
        return dict(self._response.headers.items())


class CurlImpersonateHttpClient(BaseHttpClient):
    """HTTP client based on the `curl-cffi` library.

    This client uses the `curl-cffi` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.
    """

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **async_session_kwargs: Any,
    ) -> None:
        """Create a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors.
            ignore_http_error_status_codes: HTTP status codes to ignore as errors.
            async_session_kwargs: Additional keyword arguments for `curl_cffi.requests.AsyncSession`.
        """
        self._persist_cookies_per_session = persist_cookies_per_session
        self._additional_http_error_status_codes = set(additional_http_error_status_codes)
        self._ignore_http_error_status_codes = set(ignore_http_error_status_codes)
        self._async_session_kwargs = async_session_kwargs

        self._client_by_proxy_url = dict[Optional[str], AsyncSession]()

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

        try:
            response = await client.request(
                method=request.method.upper(),  # curl-cffi requires uppercase method
                url=request.url,
                headers=request.headers,
                cookies=session.cookies if session else None,
                allow_redirects=True,
            )
        except RequestsError as exc:
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
                    f'Status code {response.status_code} (user-configured to be an error) returned',
                )

            raise HttpStatusCodeError(f'Status code {response.status_code} returned')

        request.loaded_url = response.url

        return HttpCrawlingResult(
            http_response=_CurlImpersonateResponse(response),
        )

    @override
    async def send_request(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        proxy_url = proxy_info.url if proxy_info else None
        client = self._get_client(proxy_url)

        try:
            response = await client.request(
                method=method.upper(),  # curl-cffi requires uppercase method
                url=url,
                headers=headers,
                cookies=session.cookies if session else None,
                allow_redirects=True,
            )
        except RequestsError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        return _CurlImpersonateResponse(response)

    def _get_client(self, proxy_url: str | None) -> AsyncSession:
        """Helper to get a HTTP client for the given proxy URL.

        If the client for the given proxy URL doesn't exist, it will be created and stored.
        """
        if proxy_url not in self._client_by_proxy_url:
            self._client_by_proxy_url[proxy_url] = AsyncSession(
                proxy=proxy_url,
                **self._async_session_kwargs,
            )

        return self._client_by_proxy_url[proxy_url]

    @staticmethod
    def _is_proxy_error(error: RequestsError) -> bool:
        """Helper to check whether the given error is a proxy-related error."""
        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):
            return True

        # Once https://github.com/yifeikong/curl_cffi/issues/361 is resolved, do it better.
        if 'CONNECT tunnel failed' in str(error):  # noqa: SIM103
            return True

        return False
