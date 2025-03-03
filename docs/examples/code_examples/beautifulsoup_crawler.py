import asyncio
from datetime import timedelta

from crawlee.crawlers import (
    BasicCrawlingContext,
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
)


async def main() -> None:
    # Create an instance of the BeautifulSoupCrawler class, a crawler that automatically
    # loads the URLs and parses their HTML using the BeautifulSoup library.
    crawler = BeautifulSoupCrawler(
        # On error, retry each page at most once.
        max_request_retries=1,
        # Increase the timeout for processing each page to 30 seconds.
        request_handler_timeout=timedelta(seconds=30),
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    # The handler receives a context parameter, providing various properties and
    # helper methods. Here are a few key ones we use for demonstration:
    # - request: an instance of the Request class containing details such as the URL
    #   being crawled and the HTTP method used.
    # - soup: the BeautifulSoup object containing the parsed HTML of the response.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
            'h1s': [h1.text for h1 in context.soup.find_all('h1')],
            'h2s': [h2.text for h2 in context.soup.find_all('h2')],
            'h3s': [h3.text for h3 in context.soup.find_all('h3')],
        }

        # Push the extracted data to the default dataset. In local configuration,
        # the data will be stored as JSON files in ./storage/datasets/default.
        await context.push_data(data)

    # Register pre navigation hook which will be called before each request.
    # This hook is optional and does not need to be defined at all.
    @crawler.pre_navigation_hook
    async def some_hook(context: BasicCrawlingContext) -> None:
        pass

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
