from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from crawlee._utils.http import is_status_code_error
from crawlee.errors import HttpStatusCodeError

if TYPE_CHECKING:
    from crawlee._types import HttpHeaders, HttpMethod
    from crawlee.base_storage_client._models import Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


class HttpResponse(Protocol):
    """This protocol defines the interface that any HTTP response object must implement."""

    def read(self) -> bytes:
        """Read the content of the response body."""

    @property
    def status_code(self) -> int:
        """The HTTP status code received from the server."""

    @property
    def headers(self) -> dict[str, str]:
        """The HTTP headers received in the response."""


@dataclass(frozen=True)
class HttpCrawlingResult:
    """Result of a HTTP-only crawl.

    Args:
        http_response: The HTTP response received from the server.
    """

    http_response: HttpResponse


class BaseHttpClient(ABC):
    """An abstract base class for HTTP clients used in crawlers (`BasicCrawler` subclasses)."""

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
        headers: HttpHeaders | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        """Send an HTTP request via the client.

        This method is called from `context.send_request()` helper.

        Args:
            url: The URL to send the request to.
            method: The HTTP method to use.
            headers: The headers to include in the request.
            session: The session associated with the request.
            proxy_info: The information about the proxy to be used.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.
            HttpStatusError: Raised if the response status code indicates an error.

        Returns:
            The HTTP response received from the server.
        """

    def _raise_for_error_status_code(
        self,
        status_code: int,
        additional_http_error_status_codes: set[int],
        ignore_http_error_status_codes: set[int],
    ) -> None:
        """Raise an exception if the given status code is considered as an error."""
        exclude_error = status_code in ignore_http_error_status_codes
        include_error = status_code in additional_http_error_status_codes

        if include_error or (is_status_code_error(status_code) and not exclude_error):
            if include_error:
                raise HttpStatusCodeError('Error status code (user-configured) returned.', status_code)

            raise HttpStatusCodeError('Error status code returned', status_code)
