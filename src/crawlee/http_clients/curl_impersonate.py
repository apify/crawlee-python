from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

try:
    from curl_cffi.requests import AsyncSession
    from curl_cffi.requests.errors import RequestsError
    from curl_cffi.requests.impersonate import BrowserType
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

    from crawlee._types import HttpHeaders, HttpMethod
    from crawlee.base_storage_client._models import Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


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
                url=request.url,
                method=request.method.upper(),  # curl-cffi requires uppercase method
                headers=request.headers,
                params=request.query_params,
                data=request.data,
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
        query_params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        proxy_url = proxy_info.url if proxy_info else None
        client = self._get_client(proxy_url)

        try:
            response = await client.request(
                url=url,
                method=method.upper(),  # curl-cffi requires uppercase method
                headers=headers,
                params=query_params,
                data=data,
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

        The method checks if an `AsyncSession` already exists for the provided proxy URL. If no session exists,
        it creates a new one, configured with the specified proxy and additional session options. The new session
        is then stored for future use.
        """
        # Check if a session for the given proxy URL has already been created.
        if proxy_url not in self._client_by_proxy_url:
            # Prepare a default kwargs for the new session. A provided proxy URL and a chrome for impersonation
            # are set as default options.
            kwargs: dict[str, Any] = {
                'proxy': proxy_url,
                'impersonate': BrowserType.chrome,
            }

            # Update the default kwargs with any additional user-provided kwargs.
            kwargs.update(self._async_session_kwargs)

            # Create and store the new session with the specified kwargs.
            self._client_by_proxy_url[proxy_url] = AsyncSession(**kwargs)

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
