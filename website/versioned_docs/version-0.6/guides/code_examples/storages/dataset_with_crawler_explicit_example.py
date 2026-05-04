import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import Dataset


async def main() -> None:
    # Open the dataset, if it does not exist, it will be created.
    # Leave name empty to use the default dataset.
    dataset = await Dataset.open()

    # Create a new crawler (it can be any subclass of BasicCrawler).
    crawler = BeautifulSoupCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
        }

        # Push the extracted data to the dataset.
        await dataset.push_data(data)

    # Run the crawler with the initial URLs.
    await crawler.run(['https://crawlee.dev'])

    # Export the dataset to the key-value store.
    await dataset.export_to(key='dataset', content_type='csv')


if __name__ == '__main__':
    asyncio.run(main())
