from __future__ import annotations

import contextvars
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee._types import HttpHeaders
from crawlee.crawlers._playwright._types import PlaywrightHttpResponse
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from datetime import timedelta

    from playwright.async_api import Page

    from crawlee import Request
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


_browser_page_context_var: contextvars.ContextVar[Page | None] = contextvars.ContextVar('browser_context', default=None)


@asynccontextmanager
async def browser_page_context(page: Page) -> AsyncGenerator[None, None]:
    """Asynchronous context manager for setting the current Playwright page in the context variable."""
    token = _browser_page_context_var.set(page)
    try:
        yield
    finally:
        _browser_page_context_var.reset(token)


class PlaywrightHttpClient(HttpClient):
    """HTTP client based on the Playwright library.

    This client uses the Playwright library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `HttpClient` class for more common information about HTTP clients.

    Note: This class is pre-designated for use in `PlaywrightCrawler` only
    """

    def __init__(self) -> None:
        """Initialize a new instance."""
        self._active = False

    @override
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
    ) -> HttpCrawlingResult:
        raise NotImplementedError('The `crawl` method should not be used for `PlaywrightHttpClient`')

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
    ) -> HttpResponse:
        # `proxy_info` are not used because `APIRequestContext` inherits the proxy from `BrowserContext`
        # TODO: Use `session` to restore all the fingerprint headers according to the `BrowserContext`, after resolved
        # https://github.com/apify/crawlee-python/issues/1055

        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        browser_context = _browser_page_context_var.get()

        if browser_context is None:
            raise RuntimeError('Unable to create an `APIRequestContext` outside the browser context')

        # Proxies appropriate to the browser context are used
        response = await browser_context.request.fetch(
            url_or_request=url, method=method.lower(), headers=dict(headers) if headers else None, data=payload
        )

        return await PlaywrightHttpResponse.from_playwright_response(response, protocol='')

    @override
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
        raise NotImplementedError('The `stream` method should not be used for `PlaywrightHttpClient`')

    async def cleanup(self) -> None:
        # The `browser_page_context` is responsible for resource cleanup
        return
