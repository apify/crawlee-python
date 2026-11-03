from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import timedelta
from http import HTTPStatus
from time import monotonic
from typing import TYPE_CHECKING, Self

from yarl import URL

from crawlee._types import HttpHeaders
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import validate_http_url
from crawlee.sessions._cookies import SessionCookies

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from types import TracebackType

    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session

_MAX_REDIRECTS = 10

_REDIRECT_STATUS_CODES = frozenset(
    {
        HTTPStatus.MOVED_PERMANENTLY,
        HTTPStatus.FOUND,
        HTTPStatus.SEE_OTHER,
        HTTPStatus.TEMPORARY_REDIRECT,
        HTTPStatus.PERMANENT_REDIRECT,
    }
)

# Status codes that redirect a `POST` as a `GET`. `HTTPStatus.SEE_OTHER` does so for any method but `GET` and `HEAD`.
_MOVED_STATUS_CODES = frozenset({HTTPStatus.MOVED_PERMANENTLY, HTTPStatus.FOUND})

_HTTP_SCHEMES = frozenset({'http', 'https'})

# Headers scoped to a single origin, dropped as soon as a redirect leaves it.
_CROSS_ORIGIN_HEADERS = frozenset({'authorization', 'cookie', 'proxy-authorization'})

_REQUEST_BODY_HEADERS = frozenset(
    {'content-encoding', 'content-language', 'content-location', 'content-type', 'content-length'}
)


def _with_ascii_host(url: URL) -> URL:
    """Encode a non-ASCII host of `url` with IDNA, the form in which the HTTP clients report `HttpResponse.url`.

    The cookies are stored under the URL of the response and looked up by the URL of the request, so both must spell the
    host the same way.
    """
    raw_host = url.raw_host
    if raw_host is None or raw_host.isascii():
        return url

    return url.with_host(raw_host)


def _is_cross_origin(url: URL, next_url: URL) -> bool:
    """Check whether a redirect from `url` to `next_url` leaves the origin.

    Origins are compared as lowercase strings, because `yarl` keeps the case of a host parsed with `encoded=True` and
    considers an explicitly written default port different from an omitted one.
    """
    return str(url.origin()).lower() != str(next_url.origin()).lower()


def _redirect_method(status_code: int, method: HttpMethod) -> HttpMethod:
    """Resolve the method of a redirected request, following the `HTTP-redirect fetch` algorithm.

    See https://fetch.spec.whatwg.org/#http-redirect-fetch.
    """
    if (status_code in _MOVED_STATUS_CODES and method == 'POST') or (
        status_code == HTTPStatus.SEE_OTHER and method not in {'GET', 'HEAD'}
    ):
        return 'GET'

    return method


def _add_cookie_header(headers: HttpHeaders, cookies: SessionCookies, url: str) -> HttpHeaders:
    """Add the stored cookies matching `url` as the `Cookie` header, unless the headers already have one."""
    if 'cookie' in headers or not (cookie_header := cookies.get_cookie_string(url)):
        return headers

    return headers | HttpHeaders({'cookie': cookie_header})


@docs_group('Other')
class HttpResponse(ABC):
    """Base class for the HTTP responses returned by an `HttpClient`.

    The body is not read when the response is received. Read it with `read` or `read_stream`, or release the connection
    with `aclose` when the body is not needed. Used as an async context manager, the response is closed on exit.
    """

    @property
    @abstractmethod
    def url(self) -> str:
        """The URL of the response, after all redirects."""

    @property
    @abstractmethod
    def http_version(self) -> str:
        """The HTTP version used in the response."""

    @property
    @abstractmethod
    def status_code(self) -> int:
        """The HTTP status code received from the server."""

    @property
    @abstractmethod
    def headers(self) -> HttpHeaders:
        """The HTTP headers received in the response."""

    @property
    @abstractmethod
    def headers_set_cookie(self) -> list[str]:
        """The values of all `Set-Cookie` headers of the response.

        `headers` holds one value per header name, so repeated `Set-Cookie` headers are only available here.
        """

    @abstractmethod
    async def read(self) -> bytes:
        """Read the entire content of the response body.

        This method loads the complete response body into memory at once. Repeated calls return the same content.

        Raises:
            RuntimeError: If the body was already consumed by `read_stream`.
        """

    @abstractmethod
    def read_stream(self) -> AsyncIterator[bytes]:
        """Iterate over the content of the response body in chunks.

        This method should be used to process large response bodies without loading them entirely into memory.

        Raises:
            RuntimeError: If the body was already read or streamed.
        """

    @abstractmethod
    async def aclose(self) -> None:
        """Close the response and release its connection.

        Reading the whole body releases the connection as well. Repeated calls have no effect.
        """

    async def __aenter__(self) -> Self:
        """Return the response itself."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the response."""
        await self.aclose()


@docs_group('HTTP clients')
class HttpClient(ABC):
    """An abstract base class for HTTP clients used in crawlers (`BasicCrawler` subclasses).

    A subclass implements `_fetch`, which sends a single request. `send_request` builds on it, and follows redirects and
    handles cookies the same way for every client.
    """

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        follow_redirects: bool = True,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            follow_redirects: Whether `send_request` follows redirects. Without following them, it returns the first
                response as it is.
            headers: The headers to include in every request. The headers passed to `send_request` win over them.
        """
        self._persist_cookies_per_session = persist_cookies_per_session
        self._follow_redirects = follow_redirects
        self._headers = HttpHeaders(headers or {})

        # Flag to indicate the context state.
        self._active = False

    @property
    def active(self) -> bool:
        """Indicate whether the context is active."""
        return self._active

    async def send_request(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        timeout: timedelta | None = None,
    ) -> HttpResponse:
        """Send an HTTP request via the client, following up to 10 redirects unless `follow_redirects` is disabled.

        Every request of the redirect chain sends the cookies matching its URL and stores the cookies of its response. A
        `Cookie` header passed by the caller is sent instead of the stored cookies, and its cookies are not stored. The
        `Authorization`, `Proxy-Authorization` and `Cookie` headers are dropped when a redirect leaves the origin.

        The body of the returned response is not read yet. Read it, or call `aclose` on the response to release its
        connection.

        Args:
            url: The URL to send the request to.
            method: The HTTP method to use.
            headers: The headers to include in the request.
            payload: The data to be sent as the request body.
            session: The session whose cookies are sent. The response cookies are stored in it when
                `persist_cookies_per_session` is enabled. Otherwise, and without a session, they are kept only for the
                redirect chain of this request.
            proxy_info: The information about the proxy to be used.
            timeout: Maximum time allowed to receive the response, including all redirects, and to read its body.
                Without it, the default timeout of the HTTP client applies.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.
            TimeoutError: If the response does not arrive within `timeout`. Reading the body raises it as well.
            RuntimeError: If the server redirects more than 10 times.

        Returns:
            The HTTP response received from the server.
        """
        validate_http_url(url)

        # The headers of the client go through the redirect rules below too, so its credentials never leave the origin.
        headers = self._headers | HttpHeaders(headers or {})

        if session is None:
            cookies = SessionCookies()
        elif self._persist_cookies_per_session:
            cookies = session.cookies
        else:
            # The cookies of the session are still sent, but the response cookies go to a copy.
            cookies = SessionCookies(session.cookies)

        # `encoded=True` keeps the path and the query byte for byte, including a safe `%2F` in query.
        current_url = _with_ascii_host(URL(url, encoded=True))
        deadline = monotonic() + timeout.total_seconds() if timeout is not None else None

        for _ in range(_MAX_REDIRECTS + 1):
            remaining = deadline - monotonic() if deadline is not None else None
            if remaining is not None and remaining <= 0:
                raise TimeoutError

            # One wall-clock deadline for the whole chain, whatever timeouts the client library applies on its own.
            async with asyncio.timeout(remaining):
                response = await self._fetch(
                    str(current_url),
                    method=method,
                    headers=_add_cookie_header(headers, cookies, str(current_url)),
                    payload=payload,
                    proxy_info=proxy_info,
                    timeout=timedelta(seconds=remaining) if remaining is not None else None,
                )

            try:
                set_cookie_headers = response.headers_set_cookie
                if set_cookie_headers:
                    cookies.extract_cookies_from_headers(response.url, set_cookie_headers)

                location = response.headers.get('location')
                if not self._follow_redirects or response.status_code not in _REDIRECT_STATUS_CODES or not location:
                    return response

                next_url = _with_ascii_host(current_url.join(URL(location, encoded=True)))
                if next_url.scheme not in _HTTP_SCHEMES:
                    return response
            except BaseException:
                await response.aclose()
                raise

            await response.aclose()

            next_method = _redirect_method(response.status_code, method)
            if next_method != method:
                method = next_method
                payload = None
                kept_headers = {key: value for key, value in headers.items() if key not in _REQUEST_BODY_HEADERS}
                headers = HttpHeaders(kept_headers)

            if _is_cross_origin(current_url, next_url):
                kept_headers = {key: value for key, value in headers.items() if key not in _CROSS_ORIGIN_HEADERS}
                headers = HttpHeaders(kept_headers)

            current_url = next_url

        raise RuntimeError(f'Too many redirects ({_MAX_REDIRECTS}) while requesting {url}.')

    @abstractmethod
    async def _fetch(
        self,
        url: str,
        *,
        method: HttpMethod,
        headers: HttpHeaders,
        payload: HttpPayload | None,
        proxy_info: ProxyInfo | None,
        timeout: timedelta | None,
    ) -> HttpResponse:
        """Send a single HTTP request and return the response without reading its body.

        The client must neither follow redirects nor keep cookies of its own. `send_request` does both, and passes the
        cookies in the `Cookie` header.

        Args:
            url: The URL to send the request to.
            method: The HTTP method to use.
            headers: The headers to include in the request.
            payload: The data to be sent as the request body.
            proxy_info: The information about the proxy to be used.
            timeout: Maximum time allowed to receive the response and read its body. Without it, the default timeout
                of the client library applies. `send_request` also cancels the call when the time is up.

        Raises:
            ProxyError: Raised if a proxy-related error occurs.
            TimeoutError: If the response does not arrive within `timeout`. Reading the body raises it as well.

        Returns:
            The HTTP response received from the server.
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
