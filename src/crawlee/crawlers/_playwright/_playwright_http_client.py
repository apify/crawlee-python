from __future__ import annotations

import contextvars
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.crawlers._playwright._types import PlaywrightHttpResponse
from crawlee.http_clients import HttpClient
from crawlee.sessions import Session
from crawlee.sessions._cookies import PlaywrightCookieParam

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from datetime import timedelta

    from playwright.async_api import Page

    from crawlee._types import HttpHeaders, HttpMethod, HttpPayload
    from crawlee.http_clients import HttpResponse
    from crawlee.proxy_configuration import ProxyInfo


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
        # TODO: Use `session` to restore all the fingerprint headers according to the `BrowserContext`, after resolved
        # https://github.com/apify/crawlee-python/issues/1055

        page = _browser_page_context_var.get()

        # The browser context holds the cookies of the session, added before the navigation, with the changes the page
        # made since. The browser stores the response cookies there too, and the crawler saves them to the session after
        # the request handler.
        if page is not None:
            context_cookies = await page.context.cookies()
            session = Session()
            session.cookies.set_cookies_from_playwright_format(
                [PlaywrightCookieParam(**cookie) for cookie in context_cookies]
            )

        return await super().send_request(
            url,
            method=method,
            headers=headers,
            payload=payload,
            session=session,
            proxy_info=proxy_info,
            timeout=timeout,
        )

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
    ) -> HttpResponse:
        # `proxy_info` are not used because `APIRequestContext` inherits the proxy from `BrowserContext`
        browser_context = _browser_page_context_var.get()

        if browser_context is None:
            raise RuntimeError('Unable to create an `APIRequestContext` outside the browser context')

        # Proxies appropriate to the browser context are used
        response = await browser_context.request.fetch(
            url_or_request=url,
            method=method.lower(),
            headers=dict(headers) or None,
            data=payload,
            timeout=timeout.total_seconds() * 1000 if timeout is not None else None,
            # Redirects are followed hop by hop by `send_request`.
            max_redirects=0,
        )

        return await PlaywrightHttpResponse.from_playwright_response(response, protocol='')

    async def cleanup(self) -> None:
        # The `browser_page_context` is responsible for resource cleanup
        return
