from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Protocol

if TYPE_CHECKING:
    import httpx

    from crawlee.request import Request


class HttpResponse(Protocol):
    """Response to an HTTP call."""

    def read(self) -> bytes:
        """Read the content of the response body."""

    @property
    def status_code(self) -> int:
        """HTTP status code of the response (https://developer.mozilla.org/en-US/docs/Web/HTTP/Status)."""

    @property
    def headers(self) -> httpx.Headers:  # Type from `httpx` is used here because it is lightweight and convenient
        """Response headers."""


@dataclass(frozen=True)
class HttpCrawlingResult:
    """Result of a HTTP-only crawl."""

    http_response: HttpResponse


class BaseHttpClient(ABC):
    """An HTTP client used for making HTTP calls in crawlers (`BasicCrawler` subclasses)."""

    def __init__(
        self,
        *,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        self._additional_http_error_status_codes = set(additional_http_error_status_codes)
        self._ignore_http_error_status_codes = ignore_http_error_status_codes

    @abstractmethod
    async def crawl(self, request: Request) -> HttpCrawlingResult:
        """Perform an HTTP request."""
