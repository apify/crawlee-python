import asyncio

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    # Create a new crawler (it can be any subclass of BasicCrawler). Request queue is
    # a default request manager, it will be opened, and fully managed if not specified.
    crawler = HttpCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Use context's add_requests method helper to add new requests from the handler.
        await context.add_requests(['https://crawlee.dev/python/'])

    # Use crawler's add_requests method helper to add new requests.
    await crawler.add_requests(['https://apify.com/'])

    # Run the crawler. You can optionally pass the list of initial requests.
    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
