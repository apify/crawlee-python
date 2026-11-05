import asyncio

from crawlee.crawlers import BasicCrawler, BasicCrawlingContext
from crawlee.request_loaders import ThrottlingRequestManager
from crawlee.storages import RequestQueue


async def main() -> None:
    # Throttle specific domains. Without a queue to wrap, the throttler opens the default
    # request queue on first use.
    throttler = ThrottlingRequestManager(
        domains=['api.example.com', 'slow-site.org'],
        request_manager_opener=RequestQueue.open,
    )

    # Pass the throttler as the crawler's request manager.
    crawler = BasicCrawler(request_manager=throttler)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

    # Add requests. Listed domains are routed directly to their throttled sub-managers.
    # Others go to the inner manager.
    await throttler.add_requests(
        [
            'https://api.example.com/data',
            'https://api.example.com/users',
            'https://slow-site.org/page1',
            'https://fast-site.com/page1',  # Not throttled
        ]
    )

    await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
