import asyncio
from datetime import timedelta

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import BasicCrawlingContext, HttpCrawler, HttpCrawlingContext
from crawlee.errors import SessionError
from crawlee.sessions import SessionPool


async def main() -> None:
    crawler = HttpCrawler(
        # Limit requests per minute to reduce the chance of being blocked
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=50),
        # Disable session rotation
        max_session_rotations=0,
        session_pool=SessionPool(
            # Only one session in the pool
            max_pool_size=1,
            create_session_settings={
                # High value for session usage limit
                'max_usage_count': 999_999,
                # High value for session lifetime
                'max_age': timedelta(hours=999_999),
                # High score allows the session to encounter more errors
                # before crawlee decides the session is blocked
                # Make sure you know how to handle these errors
                'max_error_score': 100,
                # 403 status usually indicates you're already blocked
                'blocked_status_codes': [403],
            },
        ),
    )

    # Basic request handling logic
    @crawler.router.default_handler
    async def basic_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

    # Handler for session initialization (authentication, initial cookies, etc.)
    @crawler.router.handler(label='session_init')
    async def session_init(context: HttpCrawlingContext) -> None:
        if context.session:
            context.log.info(f'Init session {context.session.id}')

    # Monitor if our session gets blocked and explicitly stop the crawler
    @crawler.error_handler
    async def error_processing(context: BasicCrawlingContext, error: Exception) -> None:
        if isinstance(error, SessionError) and context.session:
            context.log.info(f'Session {context.session.id} blocked')
            crawler.stop()

    await crawler.run([Request.from_url('https://example.org/', label='session_init')])


if __name__ == '__main__':
    asyncio.run(main())
