import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import KeyValueStore


async def main() -> None:
    # Open a named KeyValueStore asynchronously for shared use
    store = await KeyValueStore.open(name='shared-store')

    # Initialize the BeautifulSoupCrawler instance
    crawler = BeautifulSoupCrawler()

    # Define the handler function with access to the shared KeyValueStore
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract the title from the page
        title = context.soup.title.string if context.soup.title else 'No title'

        # Prepare data to be stored
        data = {'title': title, 'url': context.request.url}

        # Use the shared KeyValueStore instance to store data
        await store.set_value(key=context.request.url, value=data)
        context.log.info(f"Stored data in 'shared-store' for {context.request.url}")

    # Start the crawler with a list of URLs
    await crawler.run(['https://example.com'])


if __name__ == '__main__':
    asyncio.run(main())
