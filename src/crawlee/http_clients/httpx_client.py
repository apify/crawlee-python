from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, cast

import httpx
from typing_extensions import override

from .base_http_client import BaseHttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from crawlee.request import Request
    from crawlee.sessions.session import Session


class HttpTransport(httpx.AsyncHTTPTransport):
    """A modified HTTP transport adapter that avoids storing response cookies."""

    @override
    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        response = await super().handle_async_request(request)
        response.request = request

        response.extensions['_cookies'] = httpx.Cookies()
        response.extensions['_cookies'].extract_cookies(response)

        if 'Set-Cookie' in response.headers:
            del response.headers['Set-Cookie']

        return response


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
        self._client = httpx.AsyncClient(transport=HttpTransport())

    @override
    async def crawl(self, request: Request, session: Session | None) -> HttpCrawlingResult:
        http_request = self._client.build_request(
            method=request.method,
            url=request.url,
            headers=request.headers,
            cookies=session.cookies if session else None,
        )
        response = await self._client.send(http_request, follow_redirects=True)

        if self._persist_cookies_per_session and session:
            session.cookies.update(cast(httpx.Cookies, response.extensions['_cookies']))

        exclude_error = response.status_code in self._ignore_http_error_status_codes
        include_error = response.status_code in self._additional_http_error_status_codes

        if (response.is_server_error and not exclude_error) or include_error:
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
        session: Session | None = None,
    ) -> HttpResponse:
        http_request = self._client.build_request(url=url, method=method, headers=headers)
        response = await self._client.send(http_request)

        if self._persist_cookies_per_session and session:
            session.cookies.update(cast(httpx.Cookies, response.extensions['_cookies']))

        return response
