from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

import httpx
from typing_extensions import override

from .base_http_client import BaseHttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from crawlee.request import Request


class HttpxClient(BaseHttpClient):
    """A httpx-based HTTP client used for making HTTP calls in crawlers (`BasicCrawler` subclasses)."""

    def __init__(
        self,
        *,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        super().__init__(
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
        )
        self._client = httpx.AsyncClient()

    @override
    async def crawl(self, request: Request) -> HttpCrawlingResult:
        response = await self._client.request(request.method, request.url, follow_redirects=True)

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
    async def send_request(self, url: str, *, method: str, headers: httpx.Headers | dict[str, str]) -> HttpResponse:
        return await self._client.request(url=url, method=method, headers=headers)
