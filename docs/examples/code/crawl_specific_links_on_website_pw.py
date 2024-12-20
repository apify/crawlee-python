import asyncio

from crawlee import Glob
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Enqueue all the documentation links found on the page, except for the examples.
        await context.enqueue_links(
            include=[Glob('https://crawlee.dev/docs/**')],
            exclude=[Glob('https://crawlee.dev/docs/examples')],
        )

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
