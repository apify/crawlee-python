import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    # Create an instance of the BeautifulSoupCrawler class, a crawler that automatically
    # loads the URLs and parses their HTML using the BeautifulSoup library.
    crawler = BeautifulSoupCrawler()

    # Define the default request handler, which will be called for every request.
    # The handler receives a context parameter, providing various properties and
    # helper methods. Here are a few key ones we use for demonstration:
    # - request: an instance of the Request class containing details such as the URL
    #   being crawled and the HTTP method used.
    # - soup: the BeautifulSoup object containing the parsed HTML of the response.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Create custom condition to stop crawler once it finds what it is looking for.
        if 'crawlee' in context.request.url:
            crawler.stop(
                reason='Manual stop of crawler after finding `crawlee` in the url.'
            )

        # Extract data from the page.
        data = {
            'url': context.request.url,
        }

        # Push the extracted data to the default dataset. In local configuration,
        # the data will be stored as JSON files in ./storage/datasets/default.
        await context.push_data(data)

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
