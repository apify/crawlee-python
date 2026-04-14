from __future__ import annotations

import asyncio
import os
from typing import cast

from stagehand import StagehandConfig, StagehandPage

from crawlee import ConcurrencySettings
from crawlee.browsers import BrowserPool
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

from .browser_classes import StagehandPlugin
from .support_classes import CrawleeStagehand


async def main() -> None:
    # Configure local Stagehand with Gemini model
    config = StagehandConfig(
        env='LOCAL',
        model_name='google/gemini-2.5-flash-preview-05-20',
        model_api_key=os.getenv('GEMINI_API_KEY'),
    )

    # Create Stagehand instance
    stagehand = CrawleeStagehand(config)

    # Create crawler with custom browser pool using Stagehand
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
        # Custom browser pool. Gives users full control over browsers used by the crawler.
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=10),
        browser_pool=BrowserPool(
            plugins=[
                StagehandPlugin(stagehand, browser_launch_options={'headless': True})
            ],
        ),
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Cast to StagehandPage for proper type hints in IDE
        page = cast('StagehandPage', context.page)

        # Use regular Playwright method
        playwright_title = await page.title()
        context.log.info(f'Playwright page title: {playwright_title}')

        # highlight-start
        # Use AI-powered extraction with natural language
        gemini_title = await page.extract('Extract page title')
        context.log.info(f'Gemini page title: {gemini_title}')
        # highlight-end

        await context.enqueue_links()

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
