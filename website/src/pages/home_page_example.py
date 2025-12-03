import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
        headless=True,  # Run in headless mode (set to False to see the browser).
        browser_type='firefox',  # Use Firefox browser.
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page using Playwright API.
        data = {
            'url': context.request.url,
            'title': await context.page.title(),
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

        # Extract all links on the page and enqueue them.
        await context.enqueue_links()

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])

    # Export the entire dataset to a CSV file.
    await crawler.export_data('results.csv')

    # Or access the data directly.
    data = await crawler.get_data()
    crawler.log.info(f'Extracted data: {data.items}')


if __name__ == '__main__':
    asyncio.run(main())
