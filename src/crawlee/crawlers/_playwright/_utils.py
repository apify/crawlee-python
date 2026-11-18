from __future__ import annotations

import asyncio
from contextlib import suppress
from logging import getLogger
from typing import TYPE_CHECKING

from playwright.async_api import Error as PlaywrightError

if TYPE_CHECKING:
    from datetime import timedelta

    from playwright.async_api import Page, Route
    from playwright.async_api import Request as PlaywrightRequest

    from crawlee._types import HttpHeaders, HttpMethod, HttpPayload

logger = getLogger(__name__)

_DEFAULT_BLOCK_REQUEST_URL_PATTERNS = [
    '.css',
    '.webp',
    '.jpg',
    '.jpeg',
    '.png',
    '.svg',
    '.gif',
    '.woff',
    '.pdf',
    '.zip',
]

# The browser ignores these overrides on a continued request: it loads cookies from its own store and keeps proxy
# credentials away from the site.
_BROWSER_MANAGED_HEADERS = frozenset({'cookie', 'proxy-authorization'})


class NavigationRequestInterceptor:
    """One-shot page route that applies a custom method, headers, and payload to the main-frame navigation request.

    Scoping the overrides to the navigation request is the point: applying headers page-wide via
    `Page.set_extra_http_headers` would leak sensitive values like `Authorization` to every subresource request
    the page makes, including cross-origin ones. The navigation request is matched by its role (a main-frame
    navigation) rather than by its URL, because the browser normalizes URLs (adds the root path, strips
    fragments), so a URL comparison can silently miss.

    Custom headers are merged into the headers the browser would send on its own (e.g. `User-Agent` or
    fingerprint headers), with the custom ones winning.

    Redirect hops bypass routing, and a continued request passes its header overrides to them, also to a redirect
    target on another origin. So a request with a custom `Authorization` header is not continued: `Route.fetch`
    sends it without following redirects, and its response is fulfilled. The browser then follows any redirect on
    its own, without the custom headers, so the credentials never reach another origin. Without an `Authorization`
    header, the request is continued and redirect hops carry the custom headers.
    """

    def __init__(
        self,
        page: Page,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            page: The page whose navigation request gets the overrides.
            method: HTTP method of the navigation request.
            headers: Custom headers of the navigation request.
            payload: Payload of the navigation request.
            timeout: Time limit for a request sent with `Route.fetch`. If None, the Playwright default applies.
        """
        self._page = page
        self._method = method
        self._headers = headers
        self._payload = payload
        self._timeout = timeout
        self._applied = False

    async def register(self) -> None:
        """Start routing the page's requests through this interceptor.

        Once the overrides are applied, the route's predicate stops matching, so any later request
        (subresources, XHRs, client-side navigations) skips the handler entirely.
        """
        await self._page.route(lambda _: not self._applied, self._handle_route)

    async def _handle_route(self, route: Route, request: PlaywrightRequest) -> None:
        if self._applied or not request.is_navigation_request() or request.frame != self._page.main_frame:
            await route.fallback()
            return

        self._applied = True
        merged_headers = {**request.headers, **dict(self._headers)} if self._headers else None

        if merged_headers and any(name.lower() == 'authorization' for name in self._headers or ()):
            await self._fetch_and_fulfill(route, merged_headers)
            return

        await route.continue_(method=self._method, headers=merged_headers, post_data=self._payload)

    async def _fetch_and_fulfill(self, route: Route, headers: dict[str, str]) -> None:
        """Send the request with `Route.fetch` without following redirects, and fulfill the route with the response."""
        # Without these overrides, the request has the same headers as a continued one.
        headers = {name: value for name, value in headers.items() if name.lower() not in _BROWSER_MANAGED_HEADERS}

        try:
            response = await route.fetch(
                method=self._method,
                headers=headers,
                post_data=self._payload,
                max_redirects=0,
                timeout=self._timeout.total_seconds() * 1000 if self._timeout else None,
            )
            await route.fulfill(response=response)
        except PlaywrightError as exc:
            # A route that stays unresolved keeps `Page.goto` waiting for its whole timeout, and Playwright raises
            # the error of the route handler from the next call on any of its objects.
            logger.debug(f'Sending the navigation request to {route.request.url} failed: {exc}')
            with suppress(PlaywrightError):
                await route.abort()


async def infinite_scroll(page: Page) -> None:
    """Scroll to the bottom of a page, handling loading of additional items."""
    scrolled_distance = 0
    finished = False

    match_count = 0
    match_count_threshold = 4

    old_request_count = 0
    new_request_count = 0

    def track_request(request: PlaywrightRequest) -> None:
        if request.resource_type in ['xhr', 'fetch', 'websocket', 'other']:
            nonlocal new_request_count
            new_request_count += 1

    page.on('request', track_request)

    async def scroll() -> None:
        body_scroll_height = await page.evaluate('() => document.body.scrollHeight')

        delta = body_scroll_height or 10000
        await page.mouse.wheel(delta_x=0, delta_y=delta)

        nonlocal scrolled_distance
        scrolled_distance += delta

    async def check_finished() -> None:
        nonlocal old_request_count, new_request_count, match_count, finished

        while True:
            if old_request_count == new_request_count:
                match_count += 1

                if match_count >= match_count_threshold:
                    finished = True
                    return
            else:
                match_count = 0
                old_request_count = new_request_count

            await asyncio.sleep(1)

    check_task = asyncio.create_task(check_finished(), name='infinite_scroll_check_finished_task')

    try:
        while not finished:
            await scroll()
            await page.wait_for_timeout(250)
    finally:
        if not check_task.done():
            check_task.cancel()
        with suppress(asyncio.CancelledError):
            await check_task


async def block_requests(
    page: Page, url_patterns: list[str] | None = None, extra_url_patterns: list[str] | None = None
) -> None:
    """Blocks network requests matching specified URL patterns.

    Args:
        page: Playwright Page object to block requests on.
        url_patterns: List of URL patterns to block. If None, uses default patterns.
        extra_url_patterns: Additional URL patterns to append to the main patterns list.
    """
    url_patterns = list(url_patterns or _DEFAULT_BLOCK_REQUEST_URL_PATTERNS)
    url_patterns.extend(extra_url_patterns or [])

    browser_type = page.context.browser.browser_type.name if page.context.browser else 'undefined'

    if browser_type == 'chromium':
        client = await page.context.new_cdp_session(page)

        await client.send('Network.enable')
        await client.send('Network.setBlockedURLs', {'urls': url_patterns})
    else:
        extensions = [pattern.strip('*.') for pattern in url_patterns if pattern.startswith(('*.', '.'))]
        specific_files = [pattern for pattern in url_patterns if not pattern.startswith(('*.', '.'))]

        if extensions:
            await page.route(f'**/*.{{{",".join(extensions)}}}*', lambda route, _: route.abort())

        if specific_files:
            await page.route(f'**/{{{",".join(specific_files)}}}*', lambda route, _: route.abort())
