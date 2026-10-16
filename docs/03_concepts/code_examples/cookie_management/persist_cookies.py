import asyncio
from datetime import timedelta

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.sessions import SessionPool


async def main() -> None:
    # Define the pool outside the crawler so its state can be read before the run.
    # `persist_state_kvs_name` is required: an unnamed store is purged on start,
    # so persistence would silently not survive a restart.
    session_pool = SessionPool(
        max_pool_size=1,
        create_session_settings={
            # Keep the single session alive across runs.
            'max_usage_count': 999_999,
            'max_age': timedelta(hours=999_999),
            'max_error_score': 100,
        },
        persistence_enabled=True,
        persist_state_kvs_name='my-cookie-store',
    )

    async with session_pool:
        # Read a session before crawling. On the first run its jar is empty. On
        # later runs the cookies from the previous run are already restored.
        session = await session_pool.get_session()
        print(f'Cookies before run: {session.cookies.get_cookies_as_dicts()}')

        # With a single session, don't burn retries trying to rotate to another one.
        crawler = HttpCrawler(max_session_rotations=0, session_pool=session_pool)

        @crawler.router.default_handler
        async def handler(context: HttpCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url}')

        await crawler.run(['https://httpbingo.org/cookies/set?visited=1'])


if __name__ == '__main__':
    asyncio.run(main())
