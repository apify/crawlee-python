import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import Dataset


async def main() -> None:
    # Open a named dataset asynchronously for shared use across handler functions
    dataset = await Dataset.open(name='shared-store')

    # Initialize the BeautifulSoupCrawler instance
    crawler = BeautifulSoupCrawler()

    # Define the handler function with access to the shared dataset
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract relevant data from the page
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
            'html_snippet': str(context.soup)[:1000],  # First 1000 characters of HTML
        }

        # Use the shared Dataset instance to store data
        await dataset.push_data(data)
        context.log.info(f"Data stored in 'shared-dataset' for {context.request.url}")

    # Start the crawler with a list of URLs
    await crawler.run(['https://example.com'])


# Run the main function using asyncio
if __name__ == '__main__':
    asyncio.run(main())
