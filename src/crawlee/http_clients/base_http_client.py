from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Protocol

if TYPE_CHECKING:
    from crawlee.models import Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics
    from crawlee.types import HttpMethod


class HttpResponse(Protocol):
    """Protocol for HTTP responses of the HTTP client."""

    def read(self) -> bytes:
        """Read the content of the response body."""

    @property
    def status_code(self) -> int:
        """HTTP status code of the response."""

    @property
    def headers(self) -> dict[str, str]:
        """HTTP headers of the response."""


@dataclass(frozen=True)
class HttpCrawlingResult:
    """Result of a HTTP-only crawl."""

    http_response: HttpResponse


class BaseHttpClient(ABC):
    """An HTTP client used for making HTTP calls in crawlers (`BasicCrawler` subclasses)."""

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        """Create a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors.
            ignore_http_error_status_codes: HTTP status codes to ignore as errors.
        """
        self._persist_cookies_per_session = persist_cookies_per_session
        self._additional_http_error_status_codes = set(additional_http_error_status_codes)
        self._ignore_http_error_status_codes = set(ignore_http_error_status_codes)

    @abstractmethod
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
    ) -> HttpCrawlingResult:
        """Perform the crawling for a given request.

        Args:
            request: The request to be crawled.
            session: The session associated with the request.
            proxy_info: The information about the proxy to be used.
            statistics: The statistics object to register status codes.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.
            HttpStatusError: Raised if the response status code indicates an error.

        Returns:
            The result of the crawling.
        """

    @abstractmethod
    async def send_request(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: dict[str, str] | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        """Send an HTTP request via the client.

        Args:
            url: The URL to send the request to.
            method: The HTTP method to use.
            headers: The headers to include in the request.
            session: The session associated with the request.
            proxy_info: The information about the proxy to be used.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.

        Returns:
            The HTTP response received from the server.
        """

    @staticmethod
    def _is_server_code(status_code: int) -> bool:
        """Helper to determine if a status code is a server error."""
        return 500 <= status_code <= 599  # noqa: PLR2004
