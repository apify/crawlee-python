import asyncio
import json
from datetime import datetime, timedelta

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import (
    HttpCrawler,
    HttpCrawlingContext,
)
from crawlee.sessions import SessionPool


async def main() -> None:
    crawler = HttpCrawler(
        max_requests_per_crawl=10,
        # Configure to use a single persistent session throughout the crawl
        max_session_rotations=0,
        # Limit request rate to avoid triggering anti-scraping measures
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=30),
        session_pool=SessionPool(
            max_pool_size=1,
            create_session_settings={
                # Set high value to ensure the session isn't replaced during crawling
                'max_usage_count': 999_999,
                # Set high value to prevent session expiration during crawling
                'max_age': timedelta(hours=999_999),
                # Higher error tolerance before the session is considered blocked
                # Make sure you implement proper error handling in your code
                'max_error_score': 100,
            },
        ),
    )

    # Default request handler for normal page processing
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # Specialized handler for the login API request
    @crawler.router.handler('login')
    async def login_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing login at {context.request.url} ...')

        # Verify that a session is available before proceeding
        if not context.session:
            raise RuntimeError('Session not found')

        # Parse the API response containing authentication tokens and user data
        data = json.loads(await context.http_response.read())

        # Extract authentication data from the response
        token = data['token']
        expires = data['expires'].replace('Z', '+00:00')
        expires_int = int(datetime.fromisoformat(expires).timestamp())
        user_id = data['userId']
        username = data['username']

        # Set authentication cookies in the session that will be used
        # for subsequent requests
        context.session.cookies.set(name='token', value=token, expires=expires_int)
        context.session.cookies.set(name='userID', value=user_id)
        context.session.cookies.set(name='userName', value=username)

        # After successful authentication, continue crawling with the
        # authenticated session
        await context.add_requests(['https://demoqa.com/BookStore/v1/Books'])

    # Create a POST request to the authentication API endpoint
    # This will trigger the login_handler when executed
    request = Request.from_url(
        'https://demoqa.com/Account/v1/Login',
        label='login',
        method='POST',
        payload=json.dumps(
            {'userName': 'crawlee_test', 'password': 'Test1234!'}
        ).encode(),
        headers={'Content-Type': 'application/json'},
    )

    # Start the crawling process with the login request
    await crawler.run([request])


if __name__ == '__main__':
    asyncio.run(main())
