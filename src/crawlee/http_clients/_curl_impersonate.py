from __future__ import annotations

import asyncio
import warnings
from contextvars import ContextVar
from functools import cached_property
from typing import TYPE_CHECKING, Any

from curl_cffi import AsyncCurl, CurlError
from curl_cffi.const import CurlECode, CurlHttpVersion
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
from crawlee.http_clients import HttpClient, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from datetime import timedelta

    from curl_cffi import Curl
    from curl_cffi.requests import Request as CurlRequest
    from curl_cffi.requests import Response
    from curl_cffi.requests.session import HttpMethod as CurlHttpMethod

    from crawlee._types import HttpMethod
    from crawlee.proxy_configuration import ProxyInfo


_CURL_PROXY_ERRORS = [
    # Raised as a plain `ConnectionError` by `curl_cffi`, which maps only some tunnel failures to `CurlProxyError`.
    'CONNECT tunnel failed',
]
"""Content of `libcurl` proxy errors that `curl_cffi` does not report as `CurlProxyError`."""

_transfers: ContextVar[list[asyncio.Future] | None] = ContextVar('_transfers', default=None)
"""Collects the transfers started by the current `_fetch` call, so that it can abort them."""


class _EmptyCookies(CurlCookies):
    @override
    def get_cookies_for_curl(self, request: CurlRequest) -> list[CurlMorsel]:
        return []

    @override
    def update_cookies_from_curl(self, morsels: list[CurlMorsel]) -> None:
        return None

    @override
    def update_cookies_from_curl_changes(self, changes: list[bytes]) -> None:
        # The write path `curl_cffi` uses since 0.16.0; the one above is kept for older versions.
        return None


class _AsyncSession(AsyncSession):
    @override
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cookies = _EmptyCookies()


class _AsyncCurl(AsyncCurl):
    """`AsyncCurl` that hands every transfer it starts to the `_fetch` call that started it."""

    @override
    def add_handle(self, curl: Curl) -> asyncio.Future:
        transfer = super().add_handle(curl)
        if (transfers := _transfers.get()) is not None:
            transfers.append(transfer)
        return transfer


def _abort(transfer: asyncio.Future) -> None:
    """Abort a running transfer, which closes its connection and returns its handle to the pool.

    A streamed transfer of `curl_cffi` keeps running when its request is cancelled or its response closed.
    """
    if not transfer.done():
        transfer.set_exception(CurlError('The transfer was aborted.', CurlECode.ABORTED_BY_CALLBACK))


class _CurlImpersonateResponse(HttpResponse):
    """Adapter class for `curl_cffi.requests.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: Response, *, transfer: asyncio.Future, deadline: float | None) -> None:
        self._response = response
        self._transfer = transfer
        self._deadline = deadline
        # `curl_cffi` does not keep the body of a streamed response once it was read.
        self._content: bytes | None = None
        self._consumed = False

    @property
    def url(self) -> str:
        return self._response.url

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

    @cached_property
    def headers(self) -> HttpHeaders:
        return HttpHeaders({key: value for key, value in self._response.headers.items() if value})

    @property
    def headers_set_cookie(self) -> list[str]:
        return [value for value in self._response.headers.get_list('set-cookie') if value]

    async def read(self) -> bytes:
        if self._content is None:
            # `curl_cffi` returns an empty body to a second read of a failed transfer, so the body counts as consumed as
            # soon as reading starts.
            if self._consumed:
                raise RuntimeError('Stream is already consumed.')
            self._consumed = True
            try:
                async with asyncio.timeout_at(self._deadline):
                    self._content = await self._response.acontent()
            except Timeout as exc:
                raise TimeoutError from exc
        return self._content

    async def read_stream(self) -> AsyncGenerator[bytes, None]:
        if self._consumed:
            raise RuntimeError('Stream is already consumed.')
        self._consumed = True
        chunks = self._response.aiter_content()
        while True:
            try:
                async with asyncio.timeout_at(self._deadline):
                    chunk = await anext(chunks)
            except StopAsyncIteration:
                return
            except Timeout as exc:
                raise TimeoutError from exc
            yield chunk

    async def aclose(self) -> None:
        _abort(self._transfer)
        await self._response.aclose()


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
        follow_redirects: bool = True,
        **async_session_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            follow_redirects: Whether to follow HTTP redirects.
            async_session_kwargs: Additional keyword arguments for `curl_cffi.requests.AsyncSession`. Some of them are
                ignored with a warning. The `auth` and `params` arguments would reach every request of a redirect chain,
                so pass credentials in the `Authorization` header and the query in the URL instead. The client aborts
                transfers through an `AsyncCurl` of its own, so `async_curl` is ignored. Redirects are followed by
                `send_request`, so `allow_redirects` and `max_redirects` are ignored.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
            follow_redirects=follow_redirects,
            headers=async_session_kwargs.pop('headers', None),
        )

        for ignored_kwarg in ('auth', 'params', 'async_curl', 'allow_redirects', 'max_redirects'):
            if async_session_kwargs.pop(ignored_kwarg, None) is not None:
                warnings.warn(
                    f'The `{ignored_kwarg}` argument of `CurlImpersonateHttpClient` is ignored.',
                    UserWarning,
                    stacklevel=2,
                )

        self._async_session_kwargs = async_session_kwargs

        self._client_by_proxy_url = dict[str | None, AsyncSession]()

    @override
    async def _fetch(
        self,
        url: str,
        *,
        method: HttpMethod,
        headers: HttpHeaders,
        payload: HttpPayload | None,
        proxy_info: ProxyInfo | None,
        timeout: timedelta | None,
    ) -> _CurlImpersonateResponse:
        client = self._get_client(proxy_info.url if proxy_info else None)
        deadline = asyncio.get_running_loop().time() + timeout.total_seconds() if timeout is not None else None

        # Without a timeout of its own, the request gets the default timeout of the session.
        timeout_kwargs: dict[str, Any] = {}
        if timeout is not None:
            # `curl_cffi` cuts the timeout down to whole milliseconds, and `libcurl` reads 0 as no timeout.
            timeout_kwargs['timeout'] = max(timeout.total_seconds(), 0.001)

        transfers: list[asyncio.Future] = []
        token = _transfers.set(transfers)
        try:
            response = await client.request(
                url=url,
                method=self._convert_method(method),
                headers=dict(headers) or None,
                data=payload,
                # Redirects are followed hop by hop by `send_request`.
                allow_redirects=False,
                stream=True,
                **timeout_kwargs,
            )
        except asyncio.CancelledError:
            for transfer in transfers:
                _abort(transfer)
            raise
        except Timeout as exc:
            raise TimeoutError from exc
        except CurlRequestError as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise
        finally:
            _transfers.reset(token)

        # With `retry`, `curl_cffi` starts a transfer per attempt, so the live one is the last.
        return _CurlImpersonateResponse(response, transfer=transfers[-1], deadline=deadline)

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

            # Create and store the new session with the specified kwargs. Its `_AsyncCurl` lets `_fetch` abort the
            # transfers.
            self._client_by_proxy_url[proxy_url] = _AsyncSession(**kwargs, async_curl=_AsyncCurl())

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
        if any(needle in str(error) for needle in (*ROTATE_PROXY_ERRORS, *_CURL_PROXY_ERRORS)):
            return True

        if isinstance(error, CurlProxyError):  # noqa: SIM103
            return True

        return False

    async def cleanup(self) -> None:
        for client in self._client_by_proxy_url.values():
            await client.close()
            # A session does not close an `AsyncCurl` it was given.
            await client.acurl.close()
        self._client_by_proxy_url.clear()
