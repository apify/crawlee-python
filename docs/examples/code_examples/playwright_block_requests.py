import asyncio

from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        await context.enqueue_links()

    # Define the hook, which will be called before every request.
    @crawler.pre_navigation_hook
    async def navigation_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        context.log.info(f'Navigating to {context.request.url} ...')

        # Block all requests to URLs that include `adsbygoogle.js` and also all defaults.
        await context.block_requests(extra_url_patterns=['adsbygoogle.js'])

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
