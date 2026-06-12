import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
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

        # Push the extracted data to the (default) dataset.
        await context.push_data(data)

    # Run the crawler with the initial URLs.
    await crawler.run(['https://crawlee.dev'])

    # Export the dataset to a file.
    await crawler.export_data(path='dataset.csv')


if __name__ == '__main__':
    asyncio.run(main())
