import asyncio

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.request_loaders import RequestList


async def main() -> None:
    # Open the request list, if it does not exist, it will be created.
    # Leave name empty to use the default request list.
    request_list = RequestList(
        name='my-request-list',
        requests=['https://apify.com/', 'https://crawlee.dev/'],
    )

    # Join the request list into a tandem with the default request queue
    request_manager = await request_list.to_tandem()

    # Create a new crawler (it can be any subclass of BasicCrawler) and pass the request manager tandem
    crawler = HttpCrawler(request_manager=request_manager)

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Use context's add_requests method helper to add new requests from the handler.
        await context.add_requests(['https://crawlee.dev/python/docs/quick-start'])

    # Use crawler's add_requests method helper to add new requests.
    await crawler.add_requests(['https://crawlee.dev/python/api'])

    # Run the crawler. You can optionally pass the list of initial requests.
    await crawler.run(['https://crawlee.dev/python/'])


if __name__ == '__main__':
    asyncio.run(main())
