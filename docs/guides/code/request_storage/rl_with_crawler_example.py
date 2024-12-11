import asyncio

from crawlee.http_crawler import HttpCrawler, HttpCrawlingContext
from crawlee.request_sources import RequestList, RequestSourceTandem
from crawlee.storages import RequestQueue


async def main() -> None:
    # Open the request list, if it does not exist, it will be created.
    # Leave name empty to use the default request list.
    request_list = RequestList(
        name='my-request-list',
        requests=['https://apify.com/', 'https://crawlee.dev/'],
    )

    # Open a request queue that should be used for links discovered on the crawled pages.
    request_queue = await RequestQueue.open()

    # Create a new crawler (it can be any subclass of BasicCrawler) and pass the request
    # list along with a request queue joined into a tandem. It will be managed by the crawler.
    crawler = HttpCrawler(request_provider=RequestSourceTandem(request_list, request_queue))

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
