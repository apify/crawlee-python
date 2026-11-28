import asyncio
from datetime import timedelta

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import BasicCrawlingContext, HttpCrawler, HttpCrawlingContext
from crawlee.errors import RequestCollisionError
from crawlee.sessions import SessionPool


async def main() -> None:
    session_pool = SessionPool(
        max_pool_size=10,
        create_session_settings={
            'max_usage_count': 999_999,
            'max_age': timedelta(hours=999_999),
            'max_error_score': 100,
        },
    )

    crawler = HttpCrawler(
        # Adjust request limits according to your pool size
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=500),
        # 403 status usually indicates you're already blocked
        blocked_status_codes=[403],
        session_pool=session_pool,
    )

    @crawler.router.default_handler
    async def basic_handler(context: HttpCrawlingContext) -> None:
        context.log.info('Processing %s', context.request.url)

    # Initialize the session and bind the next request to this session if needed
    @crawler.router.handler(label='session_init')
    async def session_init(context: HttpCrawlingContext) -> None:
        context.log.info('Init session %s', context.session.id)
        next_request = Request.from_url(
            'https://a.placeholder.com',
            session_id=context.session.id,
            use_extended_unique_key=True,
        )
        await context.add_requests([next_request])

    # Handle errors when the session bound to a request is no longer usable
    @crawler.failed_request_handler
    async def error_processing(context: BasicCrawlingContext, error: Exception) -> None:
        if isinstance(error, RequestCollisionError):
            context.log.error(
                'Request %s failed, because the bound session is unavailable',
                context.request.url,
            )

    async with session_pool:
        # The pool creates sessions on demand, so create them before binding
        # the first requests to them, for example for authentication
        sessions = [await session_pool.get_session() for _ in range(10)]

        # Create a pool of requests bound to their respective sessions
        # Use `always_enqueue=True` if session initialization happens on a non-unique
        # address, such as the site's main page
        init_requests = [
            Request.from_url(
                'https://example.org/',
                label='session_init',
                session_id=session.id,
                use_extended_unique_key=True,
            )
            for session in sessions
        ]

        await crawler.run(init_requests)


if __name__ == '__main__':
    asyncio.run(main())
