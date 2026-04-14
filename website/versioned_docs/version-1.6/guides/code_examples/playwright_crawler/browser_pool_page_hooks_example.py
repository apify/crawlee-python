from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from crawlee.browsers import BrowserPool
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from crawlee.browsers._browser_controller import BrowserController
    from crawlee.browsers._types import CrawleePage
    from crawlee.proxy_configuration import ProxyInfo

logger = logging.getLogger(__name__)


async def main() -> None:
    async with BrowserPool() as browser_pool:

        @browser_pool.pre_page_create_hook
        async def log_page_init(
            page_id: str,
            _browser_controller: BrowserController,
            _browser_new_context_options: dict[str, Any],
            _proxy_info: ProxyInfo | None,
        ) -> None:
            """Log when a new page is about to be created."""
            logger.info(f'Creating page {page_id}...')

        @browser_pool.post_page_create_hook
        async def set_viewport(
            crawlee_page: CrawleePage, _browser_controller: BrowserController
        ) -> None:
            """Set a fixed viewport size on each newly created page."""
            await crawlee_page.page.set_viewport_size({'width': 1280, 'height': 1024})

        @browser_pool.pre_page_close_hook
        async def save_screenshot(
            crawlee_page: CrawleePage, _browser_controller: BrowserController
        ) -> None:
            """Save a screenshot to KeyValueStore before each page is closed."""
            kvs = await KeyValueStore.open()

            screenshot = await crawlee_page.page.screenshot()
            await kvs.set_value(
                key=f'screenshot-{crawlee_page.id}',
                value=screenshot,
                content_type='image/png',
            )
            logger.info(f'Saved screenshot for page {crawlee_page.id}.')

        @browser_pool.post_page_close_hook
        async def log_page_closed(
            page_id: str, _browser_controller: BrowserController
        ) -> None:
            """Log after each page is closed."""
            logger.info(f'Page {page_id} closed successfully.')

        crawler = PlaywrightCrawler(
            browser_pool=browser_pool,
            max_requests_per_crawl=5,
        )

        @crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url} ...')

            await context.enqueue_links()

        # Run the crawler with the initial list of URLs.
        await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
