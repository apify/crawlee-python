import asyncio

from crawlee.parsel_crawler import ParselCrawler, ParselCrawlingContext
from crawlee.request_sources import RequestList, RequestSourceTandem
from crawlee.storages import RequestQueue


async def main() -> None:
    request_queue = await RequestQueue.open()
    crawler = ParselCrawler(
        request_provider=RequestSourceTandem(
            # Requests from the list will be processed first, but they will be enqueued in the queue first
            RequestList(['https://crawlee.dev', 'https://apify.com']),
            request_queue,
        )
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        await context.enqueue_links()  # New links will be enqueued directly to the queue

    await crawler.run()


asyncio.run(main())
