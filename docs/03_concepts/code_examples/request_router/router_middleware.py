import asyncio
import time

from crawlee import Request
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.router import Router


async def main() -> None:
    # Create a custom router instance
    router = Router[ParselCrawlingContext]()

    # Register a middleware that logs every request before it reaches a handler
    @router.use
    async def logging_middleware(context: ParselCrawlingContext) -> None:
        context.log.info(
            f'Processing request: {context.request.url} label={context.request.label}'
        )

    # Register a middleware that adds a timestamp to the request's user data
    @router.use
    async def timestamp_middleware(context: ParselCrawlingContext) -> None:
        context.request.user_data['start_time'] = time.monotonic()

    @router.default_handler
    async def default_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} with default handler')

    @router.handler('CATEGORY')
    async def category_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} with category handler')

    crawler = ParselCrawler(
        request_handler=router,
        max_requests_per_crawl=10,
    )

    await crawler.run(
        [
            'https://warehouse-theme-metal.myshopify.com/',
            Request.from_url(
                'https://warehouse-theme-metal.myshopify.com/collections/all',
                label='CATEGORY',
            ),
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
