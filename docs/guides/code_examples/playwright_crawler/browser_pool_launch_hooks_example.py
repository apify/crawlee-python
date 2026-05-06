from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from crawlee.browsers import BrowserPool
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

if TYPE_CHECKING:
    from crawlee.browsers._browser_controller import BrowserController
    from crawlee.browsers._browser_plugin import BrowserPlugin

logger = logging.getLogger(__name__)


async def main() -> None:
    async with BrowserPool() as browser_pool:

        @browser_pool.pre_launch_hook
        async def log_browser_launch(page_id: str, plugin: BrowserPlugin) -> None:
            """Log before a new browser instance is launched."""
            logger.info(f'Launching {plugin.browser_type} browser for page {page_id}...')

        @browser_pool.post_launch_hook
        async def log_browser_launched(
            page_id: str, controller: BrowserController
        ) -> None:
            """Log after a new browser instance has been launched."""
            logger.info(f'Browser launched for page {page_id}, controller: {controller}')

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
