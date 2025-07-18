import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    # Create a ParselCrawler instance
    crawler = ParselCrawler(
        # Limit the crawl to 10 requests
        max_requests_per_crawl=10,
    )

    # Define the default request handler
    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # Extract data using Parsel's XPath and CSS selectors
        data = {
            'url': context.request.url,
            'title': context.selector.xpath('//title/text()').get(),
        }

        # Push extracted data to the dataset
        await context.push_data(data)

        # Enqueue links found on the page for further crawling
        await context.enqueue_links()

    # Run the crawler
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
