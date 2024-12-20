import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.request_loaders import RequestList


async def main() -> None:
    # Create a static request list
    request_list = RequestList(['https://crawlee.dev', 'https://apify.com'])

    crawler = ParselCrawler(
        # Requests from the list will be processed first, but they will be enqueued in the default request queue first
        request_manager=await request_list.to_tandem(),
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        await context.enqueue_links()  # New links will be enqueued directly to the queue

    await crawler.run()


asyncio.run(main())
