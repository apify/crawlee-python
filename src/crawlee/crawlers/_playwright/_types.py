from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from crawlee import HttpHeaders
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from playwright.async_api import APIResponse, Response
    from typing_extensions import Self


@docs_group('Functions')
class BlockRequestsFunction(Protocol):
    """A function for blocking unwanted HTTP requests during page loads in PlaywrightCrawler.

    It simplifies the process of blocking specific HTTP requests during page navigation.
    The function allows blocking both default resource types (like images, fonts, stylesheets) and custom URL patterns.
    """

    async def __call__(
        self, url_patterns: list[str] | None = None, extra_url_patterns: list[str] | None = None
    ) -> None:
        """Call dunder method.

        Args:
            url_patterns: List of URL patterns to block. If None, uses default patterns.
            extra_url_patterns: Additional URL patterns to append to the main patterns list.
        """


@dataclass(frozen=True)
class PlaywrightHttpResponse:
    """Wrapper class for playwright `Response` and `APIResponse` objects to implement `HttpResponse` protocol."""

    http_version: str
    status_code: int
    headers: HttpHeaders
    _content: bytes

    async def read(self) -> bytes:
        return self._content

    async def read_stream(self) -> AsyncGenerator[bytes, None]:
        # Playwright does not support `streaming` responses.
        # This is a workaround to make it compatible with `HttpResponse` protocol.
        yield self._content

    @classmethod
    async def from_playwright_response(cls, response: Response | APIResponse, protocol: str) -> Self:
        headers = HttpHeaders(response.headers)
        status_code = response.status
        # Used http protocol version cannot be obtained from `Response` and has to be passed as additional argument.
        http_version = protocol
        _content = await response.body()

        return cls(http_version=http_version, status_code=status_code, headers=headers, _content=_content)
