import asyncio
from datetime import timedelta

from crawlee.crawlers import AdaptivePlaywrightCrawler, AdaptivePlaywrightCrawlingContext


async def main() -> None:
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        # Locate element h2 within 5 seconds
        h2 = await context.query_selector_one('h2', timedelta(milliseconds=5000))
        # Do stuff with element found by the selector
        context.log.info(h2)

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
