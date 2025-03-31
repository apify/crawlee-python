import asyncio
from datetime import timedelta
from itertools import count
from typing import Callable

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import BasicCrawlingContext, HttpCrawler, HttpCrawlingContext
from crawlee.errors import RequestCollisionError
from crawlee.sessions import Session, SessionPool


# Define a function for creating sessions with simple logic for unique `id` generation.
# This is necessary if you need to specify a particular session for the first request,
# for example during authentication
def create_session_function() -> Callable[[], Session]:
    counter = count()

    def create_session() -> Session:
        return Session(
            id=str(next(counter)),
            max_usage_count=999_999,
            max_age=timedelta(hours=999_999),
            max_error_score=100,
            blocked_status_codes=[403],
        )

    return create_session


async def main() -> None:
    crawler = HttpCrawler(
        # Adjust request limits according to your pool size
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=500),
        # Requests are bound to specific sessions, no rotation needed
        max_session_rotations=0,
        session_pool=SessionPool(
            max_pool_size=10, create_session_function=create_session_function()
        ),
    )

    @crawler.router.default_handler
    async def basic_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

    # Initialize the session and bind the next request to this session if needed
    @crawler.router.handler(label='session_init')
    async def session_init(context: HttpCrawlingContext) -> None:
        next_requests = []
        if context.session:
            context.log.info(f'Init session {context.session.id}')
            next_request = Request.from_url(
                'https://placeholder.dev', session_id=context.session.id
            )
            next_requests.append(next_request)

        await context.add_requests(next_requests)

    # Handle errors when a session is blocked and no longer available in the pool
    # when attempting to execute requests bound to it
    @crawler.failed_request_handler
    async def error_processing(context: BasicCrawlingContext, error: Exception) -> None:
        if isinstance(error, RequestCollisionError) and context.session:
            context.log.error(
                f'Request {context.request.url} failed, because the bound '
                'session is unavailable'
            )

    # Create a pool of requests bound to their respective sessions
    # Use `always_enqueue=True` if session initialization happens on a non-unique address,
    # such as the site's main page
    init_requests = [
        Request.from_url(
            'https://example.org/',
            label='session_init',
            session_id=str(session_id),
            use_extended_unique_key=True,
        )
        for session_id in range(1, 11)
    ]

    await crawler.run(init_requests)


if __name__ == '__main__':
    asyncio.run(main())
