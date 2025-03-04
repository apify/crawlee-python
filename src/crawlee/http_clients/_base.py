from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from crawlee import Request
    from crawlee._types import HttpHeaders, HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


@docs_group('Data structures')
class HttpResponse(Protocol):
    """This protocol defines the interface that any HTTP response object must implement."""

    @property
    def http_version(self) -> str:
        """The HTTP version used in the response."""

    @property
    def status_code(self) -> int:
        """The HTTP status code received from the server."""

    @property
    def headers(self) -> HttpHeaders:
        """The HTTP headers received in the response."""

    def read(self) -> bytes:
        """Read the content of the response body."""


@dataclass(frozen=True)
@docs_group('Data structures')
class HttpCrawlingResult:
    """Result of an HTTP-only crawl.

    Mainly for the purpose of composing specific crawling contexts (e.g. `BeautifulSoupCrawlingContext`,
    `ParselCrawlingContext`, ...).
    """

    http_response: HttpResponse
    """The HTTP response received from the server."""


@docs_group('Abstract classes')
class HttpClient(ABC):
    """An abstract base class for HTTP clients used in crawlers (`BasicCrawler` subclasses)."""

    @abstractmethod
    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
    ) -> None:
        """A default constructor.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
        """
        self._persist_cookies_per_session = persist_cookies_per_session

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

        This method is called from `crawler.run()`.

        Args:
            request: The request to be crawled.
            session: The session associated with the request.
            proxy_info: The information about the proxy to be used.
            statistics: The statistics object to register status codes.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.

        Returns:
            The result of the crawling.
        """

    @abstractmethod
    async def send_request(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        """Send an HTTP request via the client.

        This method is called from `context.send_request()` helper.

        Args:
            url: The URL to send the request to.
            method: The HTTP method to use.
            headers: The headers to include in the request.
            payload: The data to be sent as the request body.
            session: The session associated with the request.
            proxy_info: The information about the proxy to be used.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.

        Returns:
            The HTTP response received from the server.
        """
