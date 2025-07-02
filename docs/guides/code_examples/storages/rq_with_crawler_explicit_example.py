import asyncio

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.storages import RequestQueue


async def main() -> None:
    # Open the request queue, if it does not exist, it will be created.
    # Leave name empty to use the default request queue.
    request_queue = await RequestQueue.open(name='my-request-queue')

    # Interact with the request queue directly, e.g. add a batch of requests.
    await request_queue.add_requests(['https://apify.com/', 'https://crawlee.dev/'])

    # Create a new crawler (it can be any subclass of BasicCrawler) and pass the request
    # queue as request manager to it. It will be managed by the crawler.
    crawler = HttpCrawler(request_manager=request_queue)

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # And execute the crawler.
    await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
