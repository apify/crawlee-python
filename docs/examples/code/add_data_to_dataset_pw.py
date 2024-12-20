import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': await context.page.title(),
            'html': str(await context.page.content())[:1000],
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    # Run the crawler with the initial list of requests.
    await crawler.run(
        [
            'https://crawlee.dev',
            'https://apify.com',
            'https://example.com',
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
