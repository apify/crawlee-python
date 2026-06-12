import asyncio

from crawlee import Glob
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract all the documentation links found on the page, except for the examples.
        extracted_links = await context.extract_links(
            include=[Glob('https://crawlee.dev/docs/**')],
            exclude=[Glob('https://crawlee.dev/docs/examples')],
        )
        # Some very custom filtering which can't be achieved by `extract_links` arguments.
        max_link_length = 30
        filtered_links = [
            link for link in extracted_links if len(link.url) < max_link_length
        ]
        # Add filtered links to the request queue.
        await context.add_requests(filtered_links)

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
