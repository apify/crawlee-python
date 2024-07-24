from __future__ import annotations

from typing import TYPE_CHECKING, Optional, cast

import httpx
from typing_extensions import override

from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee.basic_crawler.errors import ProxyError
from crawlee.http_clients import BaseHttpClient, HttpCrawlingResult, HttpResponse
from crawlee.sessions import Session

if TYPE_CHECKING:
    from collections.abc import Iterable

    from crawlee.models import Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.statistics import Statistics

__all__ = ['HttpxClient']


class HttpTransport(httpx.AsyncHTTPTransport):
    """A modified HTTP transport adapter that stores response cookies in a `Session` instead of the httpx client."""

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


def _is_proxy_error(error: httpx.TransportError) -> bool:
    if isinstance(error, httpx.ProxyError):
        return True

    if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
        return True

    return False


class HttpxClient(BaseHttpClient):
    """A httpx-based HTTP client used for making HTTP calls in crawlers (`BasicCrawler` subclasses)."""

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

        self._client_by_proxy_url = dict[Optional[str], httpx.AsyncClient]()

    def _get_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        if proxy_url not in self._client_by_proxy_url:
            self._client_by_proxy_url[proxy_url] = httpx.AsyncClient(
                transport=HttpTransport(),
                proxy=proxy_url,
                timeout=httpx.Timeout(10),
            )

        return self._client_by_proxy_url[proxy_url]

    @override
    async def crawl(
        self,
        request: Request,
        session: Session | None,
        proxy_info: ProxyInfo | None,
        statistics: Statistics,
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
        except httpx.TransportError as e:
            if _is_proxy_error(e):
                raise ProxyError from e

            raise

        statistics.register_status_code(response.status_code)

        exclude_error = response.status_code in self._ignore_http_error_status_codes
        include_error = response.status_code in self._additional_http_error_status_codes

        if include_error or (response.is_server_error and not exclude_error):
            if include_error:
                raise httpx.HTTPStatusError(
                    f'Status code {response.status_code} (user-configured to be an error) returned',
                    request=response.request,
                    response=response,
                )

            raise httpx.HTTPStatusError(
                f'Status code {response.status_code} returned', request=response.request, response=response
            )

        request.loaded_url = str(response.url)
        return HttpCrawlingResult(http_response=response)

    @override
    async def send_request(
        self,
        url: str,
        *,
        method: str,
        headers: httpx.Headers | dict[str, str],
        session: Session | None,
        proxy_info: ProxyInfo | None,
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
        except httpx.TransportError as e:
            if _is_proxy_error(e):
                raise ProxyError from e

            raise

        return response
