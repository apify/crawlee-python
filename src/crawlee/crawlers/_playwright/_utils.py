from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from playwright.async_api import Page
    from playwright.async_api import Request as PlaywrightRequest

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
    url_patterns = url_patterns or _DEFAULT_BLOCK_REQUEST_URL_PATTERNS

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
