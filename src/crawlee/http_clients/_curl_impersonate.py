from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from http.cookiejar import Cookie
from typing import TYPE_CHECKING, Any, cast

from curl_cffi import CurlInfo
from curl_cffi.const import CurlHttpVersion
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.cookies import Cookies as CurlCookies
from curl_cffi.requests.cookies import CurlMorsel
from curl_cffi.requests.exceptions import ProxyError as CurlProxyError
from curl_cffi.requests.exceptions import RequestException as CurlRequestError
from curl_cffi.requests.exceptions import Timeout
from curl_cffi.requests.impersonate import DEFAULT_CHROME as CURL_DEFAULT_CHROME
from typing_extensions import override

from crawlee._types import HttpHeaders, HttpMethod, HttpPayload
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee.errors import ProxyError
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from datetime import timedelta

    from curl_cffi import Curl
    from curl_cffi.requests import Request as CurlRequest
    from curl_cffi.requests import Response
    from curl_cffi.requests.session import HttpMethod as CurlHttpMethod

    from crawlee import Request
    from crawlee._types import HttpMethod
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


class _EmptyCookies(CurlCookies):
    @override
    def get_cookies_for_curl(self, request: CurlRequest) -> list[CurlMorsel]:
        return []

    @override
    def update_cookies_from_curl(self, morsels: list[CurlMorsel]) -> None:
        return None


class _AsyncSession(AsyncSession):
    @override
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cookies = _EmptyCookies()


class _CurlImpersonateResponse:
    """Adapter class for `curl_cffi.requests.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: Response) -> None:
        self._response = response

    @property
    def http_version(self) -> str:
        if self._response.http_version == CurlHttpVersion.NONE:
            return 'NONE'
        if self._response.http_version == CurlHttpVersion.V1_0:
            return 'HTTP/1.0'
        if self._response.http_version == CurlHttpVersion.V1_1:
            return 'HTTP/1.1'
        if self._response.http_version in {
            CurlHttpVersion.V2_0,
            CurlHttpVersion.V2TLS,
            CurlHttpVersion.V2_PRIOR_KNOWLEDGE,
        }:
            return 'HTTP/2'
        if self._response.http_version == CurlHttpVersion.V3:
            return 'HTTP/3'

        raise ValueError(f'Unknown HTTP version: {self._response.http_version}')

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> HttpHeaders:
        return HttpHeaders({key: value for key, value in self._response.headers.items() if value})

    async def read(self) -> bytes:
        if self._response.astream_task:
            raise RuntimeError('Use `read_stream` to read the body of the Response received from the `stream` method')

        return self._response.content

    async def read_stream(self) -> AsyncGenerator[bytes, None]:
        if not self._response.astream_task:
            raise RuntimeError('Cannot read stream, Response not obtained from `stream` method.')

        if isinstance(self._response.astream_task, asyncio.Future) and self._response.astream_task.done():
            raise RuntimeError('Cannot read stream, it was already consumed.')

        async for chunk in self._response.aiter_content():
            yield chunk


@docs_group('HTTP clients')
class CurlImpersonateHttpClient(HttpClient):
    """HTTP client based on the `curl-cffi` library.

    This client uses the `curl-cffi` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `HttpClient` class for more common information about HTTP clients.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler  # or any other HTTP client-based crawler
    from crawlee.http_clients import CurlImpersonateHttpClient

    http_client = CurlImpersonateHttpClient()
    crawler = HttpCrawler(http_client=http_client)
    ```
    """

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        **async_session_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            async_session_kwargs: Additional keyword arguments for `curl_cffi.requests.AsyncSession`.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
        )
        self._async_session_kwargs = async_session_kwargs

        self._client_by_proxy_url = dict[str | None, AsyncSession]()

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
        client = self._get_client(proxy_info.url if proxy_info else None)

        try:
            response = await client.request(
                url=request.url,
                method=self._convert_method(request.method),
                headers=request.headers,
                data=request.payload,
                cookies=session.cookies.jar if session else None,
                timeout=timeout.total_seconds() if timeout else None,
            )
        except Timeout as exc:
            raise asyncio.TimeoutError from exc
        except CurlRequestError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        if statistics:
            statistics.register_status_code(response.status_code)

        if self._persist_cookies_per_session and session and response.curl:
            response_cookies = self._get_cookies(response.curl)
            session.cookies.store_cookies(response_cookies)

        request.loaded_url = response.url

        return HttpCrawlingResult(
            http_response=_CurlImpersonateResponse(response),
        )

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
        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        proxy_url = proxy_info.url if proxy_info else None
        client = self._get_client(proxy_url)

        try:
            response = await client.request(
                url=url,
                method=self._convert_method(method),
                headers=dict(headers) if headers else None,
                data=payload,
                cookies=session.cookies.jar if session else None,
                timeout=timeout.total_seconds() if timeout else None,
            )
        except Timeout as exc:
            raise asyncio.TimeoutError from exc
        except CurlRequestError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        if self._persist_cookies_per_session and session and response.curl:
            response_cookies = self._get_cookies(response.curl)
            session.cookies.store_cookies(response_cookies)

        return _CurlImpersonateResponse(response)

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
        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        proxy_url = proxy_info.url if proxy_info else None
        client = self._get_client(proxy_url)

        try:
            response = await client.request(
                url=url,
                method=self._convert_method(method),
                headers=dict(headers) if headers else None,
                data=payload,
                cookies=session.cookies.jar if session else None,
                stream=True,
                timeout=timeout.total_seconds() if timeout else None,
            )
        except Timeout as exc:
            raise asyncio.TimeoutError from exc
        except CurlRequestError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        if self._persist_cookies_per_session and session and response.curl:
            response_cookies = self._get_cookies(response.curl)
            session.cookies.store_cookies(response_cookies)

        try:
            yield _CurlImpersonateResponse(response)
        finally:
            await response.aclose()

    def _get_client(self, proxy_url: str | None) -> AsyncSession:
        """Retrieve or create an asynchronous HTTP session for the given proxy URL.

        Check if an `AsyncSession` already exists for the specified proxy URL. If no session is found,
        create a new one with the provided proxy settings and additional session options.
        Store the new session for future use.
        """
        # Check if a session for the given proxy URL has already been created.
        if proxy_url not in self._client_by_proxy_url:
            # Prepare a default kwargs for the new session. A provided proxy URL and a chrome for impersonation
            # are set as default options.
            kwargs: dict[str, Any] = {
                'proxy': proxy_url,
                'impersonate': CURL_DEFAULT_CHROME,
            }

            # Update the default kwargs with any additional user-provided kwargs.
            kwargs.update(self._async_session_kwargs)

            # Create and store the new session with the specified kwargs.
            self._client_by_proxy_url[proxy_url] = _AsyncSession(**kwargs)

        return self._client_by_proxy_url[proxy_url]

    def _convert_method(self, method: HttpMethod) -> CurlHttpMethod:
        """Convert from Crawlee HTTP method to curl-cffi HTTP method.

        Args:
            method: Crawlee HTTP method.

        Returns:
            Corresponding curl-cffi HTTP method.

        Raises:
            ValueError: If the provided HTTP method is not supported.
        """
        method_upper = method.upper()  # curl-cffi requires uppercase methods

        match method_upper:
            case 'GET':
                return 'GET'
            case 'POST':
                return 'POST'
            case 'PUT':
                return 'PUT'
            case 'DELETE':
                return 'DELETE'
            case 'OPTIONS':
                return 'OPTIONS'
            case 'HEAD':
                return 'HEAD'
            case 'TRACE':
                return 'TRACE'
            case 'PATCH':
                return 'PATCH'
            case _:
                raise ValueError(f'HTTP method {method} is not supported in {self.__class__.__name__}.')

    @staticmethod
    def _is_proxy_error(error: CurlRequestError) -> bool:
        """Determine whether the given error is related to a proxy issue.

        Check if the error message contains known proxy-related error keywords or if it is an instance
        of `CurlProxyError`.
        """
        if any(needle in str(error) for needle in ROTATE_PROXY_ERRORS):
            return True

        if isinstance(error, CurlProxyError):  # noqa: SIM103
            return True

        return False

    @staticmethod
    def _get_cookies(curl: Curl) -> list[Cookie]:
        cookies = list[Cookie]()

        # Implementation of getinfo always returns list[bytes] for CurlInfo.COOKIELIST.
        cookie_list = cast('list[bytes]', curl.getinfo(CurlInfo.COOKIELIST))

        for curl_cookie in cookie_list:
            curl_morsel = CurlMorsel.from_curl_format(curl_cookie)
            cookie = curl_morsel.to_cookiejar_cookie()
            cookies.append(cookie)

        return cookies

    async def cleanup(self) -> None:
        for client in self._client_by_proxy_url.values():
            await client.close()
        self._client_by_proxy_url.clear()
