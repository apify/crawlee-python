import asyncio

from crawlee.parsel_crawler import ParselCrawler, ParselCrawlingContext
from crawlee.request_loaders import RequestList, RequestManagerTandem


async def main() -> None:
    crawler = ParselCrawler(
        # Requests from the list will be processed first, but they will be enqueued in the default request queue first
        request_manager=await RequestManagerTandem.from_loader(
            RequestList(['https://crawlee.dev', 'https://apify.com'])
        ),
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        await context.enqueue_links()  # New links will be enqueued directly to the queue

    await crawler.run()


asyncio.run(main())
