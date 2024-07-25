from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.errors import RequestsError
from typing_extensions import override

from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee.errors import HttpStatusCodeError, ProxyError
from crawlee.http_clients import BaseHttpClient, HttpCrawlingResult, HttpResponse
from crawlee.types import HttpMethod  # noqa: TCH001

if TYPE_CHECKING:
    from collections.abc import Iterable

    from curl_cffi.requests import Response

    from crawlee.models import Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


class _CurlCffiResponse:
    """Adapter class for `curl_cffi.requests.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: Response) -> None:
        self._response = response

    def read(self) -> bytes:
        """Read the content of the response body."""
        return self._response.content

    @property
    def status_code(self) -> int:
        """HTTP status code of the response."""
        return self._response.status_code

    @property
    def headers(self) -> dict[str, str]:
        """HTTP headers of the response."""
        return dict(self._response.headers.items())


class CurlCffiHttpClient(BaseHttpClient):
    """A `curl-cffi` based HTTP client used for making HTTP calls in crawlers (`BasicCrawler` subclasses)."""

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
        )

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

        response = await client.request(
            method=request.method.upper(),  # curl-cffi requires uppercase method
            url=request.url,
            headers=request.headers,
            cookies=session.cookies if session else None,
            allow_redirects=True,
        )

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
            http_response=_CurlCffiResponse(response),
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
        headers = headers or {}
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

        return _CurlCffiResponse(response)

    def _get_client(self, proxy_url: str | None) -> AsyncSession:
        """Helper to get a HTTP client for the given proxy URL.

        If the client for the given proxy URL doesn't exist, it will be created and stored.
        """
        if proxy_url not in self._client_by_proxy_url:
            self._client_by_proxy_url[proxy_url] = AsyncSession(proxy=proxy_url, timeout=10)

        return self._client_by_proxy_url[proxy_url]

    @staticmethod
    def _is_proxy_error(error: RequestsError) -> bool:
        """Helper to check whether the given error is a proxy-related error."""
        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
            return True

        return False
