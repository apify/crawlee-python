from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from http import HTTPStatus
from logging import getLogger
from time import monotonic
from typing import TYPE_CHECKING, Any

from impit import AsyncClient, Browser, HTTPError, Response, TimeoutException, TooManyRedirects, TransportError
from impit import ProxyError as ImpitProxyError
from typing_extensions import override
from yarl import URL

from crawlee._types import HttpHeaders
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import validate_http_url
from crawlee.errors import ProxyError
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from datetime import timedelta

    from crawlee import Request
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics

logger = getLogger(__name__)

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


def _is_cross_origin(url: URL, next_url: URL) -> bool:
    """Check whether a redirect from `url` to `next_url` leaves the origin.

    Origins are compared as strings, because `yarl` considers an explicitly written default port different from
    an omitted one.
    """
    return str(url.origin()) != str(next_url.origin())


def _redirect_method(status_code: int, method: str) -> str:
    """Resolve the method of a redirected request, following the `HTTP-redirect fetch` algorithm.

    See https://fetch.spec.whatwg.org/#http-redirect-fetch.
    """
    if (status_code in _MOVED_STATUS_CODES and method == 'POST') or (
        status_code == HTTPStatus.SEE_OTHER and method not in {'GET', 'HEAD'}
    ):
        return 'GET'

    return method


class _ImpitResponse:
    """Adapter class for `impit.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: Response) -> None:
        self._response = response

    @property
    def http_version(self) -> str:
        return str(self._response.http_version)

    @property
    def status_code(self) -> int:
        return int(self._response.status_code)

    @property
    def headers(self) -> HttpHeaders:
        return HttpHeaders(dict(self._response.headers))

    async def read(self) -> bytes:
        if not self._response.is_closed:
            raise RuntimeError('Use `read_stream` to read the body of the Response received from the `stream` method')
        return self._response.content

    async def read_stream(self) -> AsyncIterator[bytes]:
        if self._response.is_stream_consumed:
            raise RuntimeError('Stream is already consumed.')
        else:
            async for chunk in self._response.aiter_bytes():
                yield chunk


@docs_group('HTTP clients')
class ImpitHttpClient(HttpClient):
    """HTTP client based on the `impit` library.

    This client uses the `impit` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `HttpClient` class for more common information about HTTP clients.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler  # or any other HTTP client-based crawler
    from crawlee.http_clients import ImpitHttpClient

    http_client = ImpitHttpClient()
    crawler = HttpCrawler(http_client=http_client)
    ```
    """

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        http3: bool = False,
        verify: bool = True,
        browser: Browser | None = 'firefox',
        follow_redirects: bool = True,
        max_redirects: int = 20,
        **async_client_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            http3: Whether to enable HTTP/3 support.
            verify: SSL certificates used to verify the identity of requested hosts.
            browser: Browser to impersonate.
            follow_redirects: Whether to follow HTTP redirects.
            max_redirects: Maximum number of redirects to follow before raising `impit.TooManyRedirects`.
            async_client_kwargs: Additional keyword arguments for `impit.AsyncClient`.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
        )
        self._http3 = http3
        self._verify = verify
        self._browser = browser
        self._follow_redirects = follow_redirects
        self._max_redirects = max_redirects

        self._async_client_kwargs = async_client_kwargs

        self._client_by_proxy_url = dict[str | None, AsyncClient]()

    @override
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
        timeout: timedelta | None = None,
    ) -> HttpCrawlingResult:
        try:
            response = await self._request_with_redirects(
                method=request.method,
                url=request.url,
                headers=dict(request.headers) if request.headers else {},
                payload=request.payload,
                session=session,
                proxy_info=proxy_info,
                timeout=timeout,
            )
        except TimeoutException as exc:
            raise asyncio.TimeoutError from exc
        except (TransportError, HTTPError) as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        if statistics:
            statistics.register_status_code(response.status_code)

        request.loaded_url = str(response.url)

        return HttpCrawlingResult(http_response=_ImpitResponse(response))

    @override
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
        validate_http_url(url)

        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        try:
            response = await self._request_with_redirects(
                method=method,
                url=url,
                headers=dict(headers),
                payload=payload,
                session=session,
                proxy_info=proxy_info,
                timeout=timeout,
            )
        except TimeoutException as exc:
            raise asyncio.TimeoutError from exc
        except (TransportError, HTTPError) as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        return _ImpitResponse(response)

    @asynccontextmanager
    @override
    async def stream(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[HttpResponse]:
        validate_http_url(url)

        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        try:
            response = await self._request_with_redirects(
                method=method,
                url=url,
                headers=dict(headers),
                payload=payload,
                session=session,
                proxy_info=proxy_info,
                timeout=timeout,
                stream=True,
            )
        except TimeoutException as exc:
            raise asyncio.TimeoutError from exc

        try:
            yield _ImpitResponse(response)
        finally:
            response.close()

    async def _request_with_redirects(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        payload: HttpPayload | None,
        session: Session | None,
        proxy_info: ProxyInfo | None,
        timeout: timedelta | None,
        stream: bool = False,
    ) -> Response:
        """Perform a request, following redirects one hop at a time.

        Redirects are resolved here instead of by `impit`, so that cookies of the given session are attached to and
        collected from every single hop. A client is therefore never bound to a session and can be shared by all
        of them.

        Args:
            method: The HTTP method to use.
            url: The URL to send the request to.
            headers: The headers to include in the request.
            payload: The data to be sent as the request body.
            session: The session whose cookies are sent and updated.
            proxy_info: The information about the proxy to be used.
            timeout: Maximum time allowed to process the request.
            stream: Whether the body of the final response should be streamed.

        Raises:
            TooManyRedirects: If the number of redirects exceeds `max_redirects`.

        Returns:
            The final response of the redirect chain.
        """
        client = self._get_client(proxy_info.url if proxy_info else None)
        # `encoded=True` keeps the URL byte for byte and safe `%2F` in query.
        current_url = URL(url, encoded=True)
        content = payload
        # The timeout bounds the whole chain rather than each hop, which is how `impit` treats it as well.
        deadline = monotonic() + timeout.total_seconds() if timeout is not None else None

        for _ in range(self._max_redirects + 1):
            remaining = deadline - monotonic() if deadline is not None else None
            if remaining is not None and remaining <= 0:
                raise asyncio.TimeoutError

            # Rebuilt per hop, so that the `Cookie` header never reaches a URL its cookies do not match. A header
            # passed by the caller wins over the session, which is how `impit` treats its own cookie jar.
            request_headers = dict(headers)
            if (
                session
                and 'cookie' not in request_headers
                and (cookie_string := session.cookies.get_cookie_string(str(current_url)))
            ):
                request_headers['cookie'] = cookie_string

            response = await client.request(
                method=method,
                url=str(current_url),
                content=content,
                headers=request_headers or None,
                timeout=remaining,
                stream=stream,
            )

            if session and self._persist_cookies_per_session:
                session.cookies.extract_cookies_from_headers(str(current_url), response.headers.get_list('set-cookie'))

            if not self._follow_redirects or response.status_code not in _REDIRECT_STATUS_CODES:
                return response

            location = response.headers.get('location')
            if not location:
                return response

            next_url = current_url.join(URL(location, encoded=True))
            if next_url.scheme not in _HTTP_SCHEMES:
                return response

            next_method = _redirect_method(response.status_code, method)
            if next_method != method:
                method = next_method
                content = None
                headers = {key: value for key, value in headers.items() if key not in _REQUEST_BODY_HEADERS}

            if _is_cross_origin(current_url, next_url):
                headers = {key: value for key, value in headers.items() if key not in _CROSS_ORIGIN_HEADERS}

            if stream:
                response.close()

            current_url = next_url

        raise TooManyRedirects(f'Exceeded the limit of {self._max_redirects} redirects while requesting {url}.')

    def _get_client(self, proxy_url: str | None) -> AsyncClient:
        """Retrieve or create an HTTP client for the given proxy URL.

        If a client for the specified proxy URL does not exist, create and store a new one.
        """
        if client := self._client_by_proxy_url.get(proxy_url):
            return client

        # Prepare a default kwargs for the new client.
        kwargs: dict[str, Any] = {
            'proxy': proxy_url,
            'http3': self._http3,
            'verify': self._verify,
            'browser': self._browser,
        }

        # Update the default kwargs with any additional user-provided kwargs.
        kwargs.update(self._async_client_kwargs)

        # Redirects are followed hop by hop by `_request_with_redirects`.
        client = AsyncClient(**kwargs, follow_redirects=False)

        self._client_by_proxy_url[proxy_url] = client

        return client

    @staticmethod
    def _is_proxy_error(error: HTTPError) -> bool:
        """Determine whether the given error is related to a proxy issue.

        Check if the error message contains known proxy-related error keywords.
        """
        if isinstance(error, ImpitProxyError):
            return True

        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):  # noqa: SIM103
            return True

        return False

    @override
    async def cleanup(self) -> None:
        """Clean up resources used by the HTTP client."""
        self._client_by_proxy_url.clear()
