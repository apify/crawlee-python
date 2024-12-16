import asyncio

from crawlee.parsel_crawler import ParselCrawler, ParselCrawlingContext
from crawlee.request_sources import RequestList, RequestSourceTandem


async def main() -> None:
    crawler = ParselCrawler(
        # Requests from the list will be processed first, but they will be enqueued in the default request queue first
        request_provider=await RequestSourceTandem.from_source(
            RequestList(['https://crawlee.dev', 'https://apify.com'])
        ),
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        await context.enqueue_links()  # New links will be enqueued directly to the queue

    await crawler.run()


asyncio.run(main())
