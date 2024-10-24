import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import Dataset


async def main() -> None:
    crawler = BeautifulSoupCrawler()
    # Open dataset manually using asynchronous constructor open().
    dataset = await Dataset.open()

    # A decorator that registers a default handler, invoked for requests without a label or with an unrecognized label.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
            'html': str(context.soup)[:1000],
        }

        # Push the extracted data to the dataset.
        await dataset.push_data(data)


if __name__ == '__main__':
    asyncio.run(main())
