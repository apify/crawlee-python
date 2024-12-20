from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from crawlee._utils.docs import docs_group
from crawlee._utils.http import is_status_code_client_error, is_status_code_server_error
from crawlee.errors import HttpClientStatusCodeError, HttpStatusCodeError

if TYPE_CHECKING:
    from collections.abc import Iterable

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
class BaseHttpClient(ABC):
    """An abstract base class for HTTP clients used in crawlers (`BasicCrawler` subclasses).

    The specific HTTP client should use `_raise_for_error_status_code` method for checking the status code. This
    way the consistent behaviour accross different HTTP clients can be maintained. It raises an `HttpStatusCodeError`
    when it encounters an error response, defined by default as any HTTP status code in the range of 400 to 599.
    The error handling behavior is customizable, allowing the user to specify additional status codes to treat as
    errors or to exclude specific status codes from being considered errors. See `additional_http_error_status_codes`
    and `ignore_http_error_status_codes` arguments in the constructor.
    """

    @abstractmethod
    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        """A default constructor.

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
        is_ignored_status = status_code in ignore_http_error_status_codes
        is_explicit_error = status_code in additional_http_error_status_codes

        if is_explicit_error:
            raise HttpStatusCodeError('Error status code (user-configured) returned.', status_code)

        if is_status_code_client_error(status_code) and not is_ignored_status:
            raise HttpClientStatusCodeError('Client error status code returned', status_code)

        if is_status_code_server_error(status_code) and not is_ignored_status:
            raise HttpStatusCodeError('Error status code returned', status_code)

    @property
    def additional_blocked_status_codes(self) -> set[int]:
        return self._additional_http_error_status_codes

    @property
    def ignore_http_error_status_codes(self) -> set[int]:
        return self._ignore_http_error_status_codes
