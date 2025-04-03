import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.request_loaders import RequestList


async def main() -> None:
    # Create a static request list.
    request_list = RequestList(['https://crawlee.dev', 'https://apify.com'])

    # Convert the request list to a request manager using the to_tandem method.
    # It is a tandem with the default request queue.
    request_manager = await request_list.to_tandem()

    # Create a crawler and pass the request manager to it.
    crawler = ParselCrawler(request_manager=request_manager)

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        # New links will be enqueued directly to the queue.
        await context.enqueue_links()

    await crawler.run()


asyncio.run(main())
