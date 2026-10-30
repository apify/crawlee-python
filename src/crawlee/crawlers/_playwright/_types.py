from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol, TypedDict

from playwright.async_api import APIResponse

from crawlee import HttpHeaders
from crawlee._utils.docs import docs_group
from crawlee.http_clients import HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import NotRequired, Self

    from playwright.async_api import Response


@docs_group('Functions')
class BlockRequestsFunction(Protocol):
    """A function for blocking unwanted HTTP requests during page loads in PlaywrightCrawler.

    It simplifies the process of blocking specific HTTP requests during page navigation.
    The function allows blocking both default resource types (like images, fonts, stylesheets) and custom URL patterns.
    """

    async def __call__(
        self, *, url_patterns: list[str] | None = None, extra_url_patterns: list[str] | None = None
    ) -> None:
        """Call dunder method.

        Args:
            url_patterns: List of URL patterns to block. If None, uses default patterns.
            extra_url_patterns: Additional URL patterns to append to the main patterns list.
        """


class PlaywrightHttpResponse(HttpResponse):
    """Wrapper class for playwright `Response` and `APIResponse` objects to implement `HttpResponse`."""

    def __init__(
        self,
        *,
        url: str,
        http_version: str,
        status_code: int,
        headers: HttpHeaders,
        headers_set_cookie: list[str],
        content: bytes,
    ) -> None:
        self._url = url
        self._http_version = http_version
        self._status_code = status_code
        self._headers = headers
        self._headers_set_cookie = headers_set_cookie
        self._content = content

    @property
    def url(self) -> str:
        return self._url

    @property
    def http_version(self) -> str:
        return self._http_version

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def headers(self) -> HttpHeaders:
        return self._headers

    @property
    def headers_set_cookie(self) -> list[str]:
        return self._headers_set_cookie

    async def read(self) -> bytes:
        return self._content

    async def read_stream(self) -> AsyncGenerator[bytes, None]:
        # Playwright does not support `streaming` responses.
        # This is a workaround to make it compatible with `HttpResponse`.
        yield self._content

    async def aclose(self) -> None:
        # The body is read when the instance is created, so there is no connection to release.
        return

    @classmethod
    async def from_playwright_response(cls, response: Response | APIResponse, protocol: str) -> Self:
        headers = HttpHeaders(response.headers)
        # Unlike `headers`, the headers array lists every `Set-Cookie` header separately.
        headers_array = response.headers_array if isinstance(response, APIResponse) else await response.headers_array()
        headers_set_cookie = [header['value'] for header in headers_array if header['name'].lower() == 'set-cookie']
        status_code = response.status
        # Used http protocol version cannot be obtained from `Response` and has to be passed as additional argument.
        http_version = protocol
        _content = await response.body()
        # If not called then the body will stay in memory until the context closes.
        if isinstance(response, APIResponse):
            await response.dispose()

        return cls(
            url=response.url,
            http_version=http_version,
            status_code=status_code,
            headers=headers,
            headers_set_cookie=headers_set_cookie,
            content=_content,
        )


class GotoOptions(TypedDict):
    """Keyword arguments for Playwright's `Page.goto()` method."""

    wait_until: NotRequired[Literal['domcontentloaded', 'load', 'networkidle', 'commit']]
    """When to consider operation succeeded, defaults to 'load' event."""

    referer: NotRequired[str]
    """Referer header value."""
