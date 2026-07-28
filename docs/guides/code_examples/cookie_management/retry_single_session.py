import asyncio
from datetime import timedelta

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.sessions import SessionPool


async def main() -> None:
    crawler = HttpCrawler(
        # With a single session, don't burn retries trying to rotate to another one.
        max_session_rotations=0,
        session_pool=SessionPool(
            # A single session, so every attempt runs on the same one.
            max_pool_size=1,
            create_session_settings={
                # Keep the single session alive for the whole crawl.
                'max_usage_count': 999_999,
                'max_age': timedelta(hours=999_999),
                'max_error_score': 100,
            },
        ),
    )

    @crawler.router.default_handler
    async def handler(context: HttpCrawlingContext) -> None:
        if context.session is None:
            return

        # First attempt: establish cookies, then raise to trigger a retry.
        if not context.session.cookies.get_cookies_as_dicts():
            await context.send_request('https://httpbingo.org/cookies/set?logged_in=1')
            raise RuntimeError('retry with cookies')

        # The retry runs on the same session, so the cookies are still here.
        context.log.info(f'Cookies: {context.session.cookies.get_cookies_as_dicts()}')

    await crawler.run(['https://httpbingo.org/cookies'])


if __name__ == '__main__':
    asyncio.run(main())
