from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager
    from datetime import timedelta
    from types import TracebackType

    from crawlee import Request
    from crawlee._types import HttpHeaders, HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


@docs_group('Data structures')
class HttpResponse(Protocol):
    """Define the interface that any HTTP response object must implement."""

    @property
    def http_version(self) -> str:
        """The HTTP version used in the response."""

    @property
    def status_code(self) -> int:
        """The HTTP status code received from the server."""

    @property
    def headers(self) -> HttpHeaders:
        """The HTTP headers received in the response."""

    async def read(self) -> bytes:
        """Read the entire content of the response body.

        This method loads the complete response body into memory at once. It should be used
        for responses received from regular HTTP requests (via `send_request` or `crawl` methods).

        Raises:
            RuntimeError: If called on a response received from the `stream` method.
        """

    def read_stream(self) -> AsyncIterator[bytes]:
        """Iterate over the content of the response body in chunks.

        This method should be used for responses received from the `stream` method to process
        large response bodies without loading them entirely into memory. It allows for efficient
        processing of potentially large data by yielding chunks sequentially.

        Raises:
            RuntimeError: If the stream has already been consumed or if the response was not obtained from the `stream`
                method.
        """


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
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
        """
        self._persist_cookies_per_session = persist_cookies_per_session

        # Flag to indicate the context state.
        self._active = False

    @property
    def active(self) -> bool:
        """Indicate whether the context is active."""
        return self._active

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

    @abstractmethod
    def stream(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        timeout: timedelta | None = None,
    ) -> AbstractAsyncContextManager[HttpResponse]:
        """Stream an HTTP request via the client.

        This method should be used for downloading potentially large data where you need to process
        the response body in chunks rather than loading it entirely into memory.

        Args:
            url: The URL to send the request to.
            method: The HTTP method to use.
            headers: The headers to include in the request.
            payload: The data to be sent as the request body.
            session: The session associated with the request.
            proxy_info: The information about the proxy to be used.
            timeout: The maximum time to wait for establishing the connection.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.

        Returns:
            An async context manager yielding the HTTP response with streaming capabilities.
        """

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up resources used by the client.

        This method is called when the client is no longer needed and should be overridden
        in subclasses to perform any necessary cleanup such as closing connections,
        releasing file handles, or other resource deallocation.
        """

    async def __aenter__(self) -> HttpClient:
        """Initialize the client when entering the context manager.

        Raises:
            RuntimeError: If the context manager is already active.
        """
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True
        return self

    async def __aexit__(
        self, exc_type: BaseException | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Deinitialize the client and clean up resources when exiting the context manager.

        Raises:
            RuntimeError: If the context manager is already active.
        """
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        await self.cleanup()
        self._active = False
