import asyncio
from datetime import timedelta

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
)
from crawlee.sessions import SessionPool


async def main() -> None:
    crawler = PlaywrightCrawler(
        max_requests_per_crawl=10,
        headless=True,
        browser_type='chromium',
        # We only have one session and it shouldn't rotate
        max_session_rotations=0,
        # Limit crawling intensity to avoid blocking
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=30),
        session_pool=SessionPool(
            # Limit the pool to one session
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
            },
        ),
    )

    # The main handler for processing requests
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # A handler for the login page
    @crawler.router.handler('login')
    async def login_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing login {context.request.url} ...')

        # Check if the session is available
        if not context.session:
            raise RuntimeError('Session not found')

        # Entering data into the form, `delay` to simulate human typing
        # Without this, the data will be entered instantly
        await context.page.type('#userName', 'crawlee_test', delay=100)
        await context.page.type('#password', 'Test1234!', delay=100)
        await context.page.click('#login', delay=100)

        # Wait for an element confirming that we have successfully
        # logged in to the site
        await context.page.locator('#userName-value').first.wait_for(state='visible')
        context.log.info('Login successful!')

        # Moving on to the basic flow of crawling
        await context.add_requests(['https://demoqa.com/books'])

    # We start crawling with login. This is necessary to access the rest of the pages
    await crawler.run([Request.from_url('https://demoqa.com/login', label='login')])


if __name__ == '__main__':
    asyncio.run(main())
