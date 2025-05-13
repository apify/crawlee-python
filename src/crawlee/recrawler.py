from crawlee.configuration import Configuration
from crawlee.crawlers._beautifulsoup._beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import RequestQueue

"""Creates a BeautifulSoupCrawler instance that starts clean everytime and doesn't
    keep the data after finishing."""
crawler = BeautifulSoupCrawler(
    configuration=Configuration(
        persist_storage=False,
        purge_on_start=True,
    ),
)

"""Logs URL of each page it visits, and tells Crawlee to keep crawling any other links
    it finds on that page."""


@crawler.router.default_handler
async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
    """Handle incoming requests and enqueue links found in the response."""
    context.log.info(f'Processing {context.request.url} ...')
    # Enqueue links without specifying a strategy
    await context.enqueue_links()

    # Opens and closes a request queue to make sure no requests from a previous
    # run interfere with the current run
    request_provider = await RequestQueue.open()
    await request_provider.drop()


if __name__ == '__main__':
    # Does not run because of BeautifulSoup parser errors - must be run in jupyter notebook
    import asyncio

    asyncio.run(crawler.run(['https://11-19-inject-broken-links.docs-7kl.pages.dev']))
