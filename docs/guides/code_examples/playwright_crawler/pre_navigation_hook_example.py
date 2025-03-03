import asyncio

from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = PlaywrightCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        await context.enqueue_links()

    @crawler.pre_navigation_hook
    async def log_navigation_url(context: PlaywrightPreNavCrawlingContext) -> None:
        context.log.info(f'Navigating to {context.request.url} ...')

        # will set a timeout for all navigation methods
        context.page.set_default_navigation_timeout(600_000)

        # will set the page size before you go to the target URL
        await context.page.set_viewport_size({'width': 1280, 'height': 1024})

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
