import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Setting the strategy to SAME_HOSTNAME will enqueue all links found that are on
        # the same hostname (including subdomains) as request.loaded_url or request.url.
        await context.enqueue_links(strategy=EnqueueStrategy.SAME_HOSTNAME)

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
