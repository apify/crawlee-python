from __future__ import annotations

import asyncio
import warnings
from functools import cached_property
from logging import getLogger
from typing import TYPE_CHECKING, Any

from impit import USE_CLIENT_DEFAULT, AsyncClient, Browser, HTTPError, Response, TimeoutException, TransportError
from impit import ProxyError as ImpitProxyError
from typing_extensions import override

from crawlee._types import HttpHeaders
from crawlee._utils.blocked import ROTATE_PROXY_ERRORS
from crawlee._utils.docs import docs_group
from crawlee._utils.http import drain_small_body
from crawlee.errors import ProxyError
from crawlee.http_clients import HttpClient, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import timedelta

    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo

logger = getLogger(__name__)


class _ImpitResponse(HttpResponse):
    """Adapter class for `impit.Response` to implement `HttpResponse`."""

    def __init__(self, response: Response, *, deadline: float | None) -> None:
        self._response = response
        self._deadline = deadline
        self._streamed = False
        # A repeated `aread()` of `impit` still goes through the event loop, so past the deadline it would time out.
        self._content: bytes | None = None

    @property
    def url(self) -> str:
        return str(self._response.url)

    @property
    def http_version(self) -> str:
        return str(self._response.http_version)

    @property
    def status_code(self) -> int:
        return int(self._response.status_code)

    @cached_property
    def headers(self) -> HttpHeaders:
        return HttpHeaders(dict(self._response.headers))

    @property
    def headers_set_cookie(self) -> list[str]:
        return self._response.headers.get_list('set-cookie')

    async def read(self) -> bytes:
        if self._streamed:
            raise RuntimeError('Stream is already consumed.')
        if self._content is None:
            try:
                async with asyncio.timeout_at(self._deadline):
                    self._content = await self._response.aread()
            except TimeoutException as exc:
                raise TimeoutError from exc
        return self._content

    async def read_stream(self) -> AsyncIterator[bytes]:
        if self._streamed or self._response.is_stream_consumed:
            raise RuntimeError('Stream is already consumed.')
        self._streamed = True
        chunks = self._response.aiter_bytes()
        while True:
            try:
                async with asyncio.timeout_at(self._deadline):
                    chunk = await anext(chunks)
            except StopAsyncIteration:
                return
            except TimeoutException as exc:
                raise TimeoutError from exc
            yield chunk

    async def aclose(self) -> None:
        try:
            await drain_small_body(self)
        finally:
            await self._response.aclose()


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
        **async_client_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            http3: Whether to enable HTTP/3 support.
            verify: SSL certificates used to verify the identity of requested hosts.
            browser: Browser to impersonate.
            follow_redirects: Whether to follow HTTP redirects.
            async_client_kwargs: Additional keyword arguments for `impit.AsyncClient`. Some of them are ignored with a
                warning. Cookies are kept in sessions, so `cookies` and `cookie_jar` are ignored. Redirects are followed
                by `send_request`, so `max_redirects` is ignored.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
            follow_redirects=follow_redirects,
            headers=async_client_kwargs.pop('headers', None),
        )
        self._http3 = http3
        self._verify = verify
        self._browser = browser

        for ignored_kwarg in ('cookies', 'cookie_jar', 'max_redirects'):
            if async_client_kwargs.pop(ignored_kwarg, None) is not None:
                warnings.warn(
                    f'The `{ignored_kwarg}` argument of `ImpitHttpClient` is ignored.',
                    UserWarning,
                    stacklevel=2,
                )

        self._async_client_kwargs = async_client_kwargs

        self._client_by_proxy_url = dict[str | None, AsyncClient]()

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
    ) -> _ImpitResponse:
        client = self._get_client(proxy_info.url if proxy_info else None)
        deadline = asyncio.get_running_loop().time() + timeout.total_seconds() if timeout is not None else None

        try:
            response = await client.request(
                method=method,
                url=url,
                content=payload,
                headers=dict(headers) or None,
                # `None` would disable the timeout, the client default applies instead.
                timeout=timeout.total_seconds() if timeout is not None else USE_CLIENT_DEFAULT,
                stream=True,
            )
        except TimeoutException as exc:
            raise TimeoutError from exc
        except (TransportError, HTTPError) as exc:
            if self._is_proxy_error(exc):
                raise ProxyError from exc
            raise

        return _ImpitResponse(response, deadline=deadline)

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

        # Redirects are followed hop by hop by `send_request`.
        kwargs['follow_redirects'] = False

        client = AsyncClient(**kwargs)

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
