import asyncio

from crawlee.adaptive_crawler._adaptive_playwright_crawler import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightCrawlingContext,
)


async def main() -> None:
    """Example of adaptive playwright crawler."""
    adaptive_crawler = await AdaptivePlaywrightCrawler.create_with_default_settings(max_crawl_depth=2)

    @adaptive_crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing with Top adaptive_crawler: {context.request.url} ...')

        await context.enqueue_links()
        await context.push_data({'Top crwaler Url': context.request.url})



    await adaptive_crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
