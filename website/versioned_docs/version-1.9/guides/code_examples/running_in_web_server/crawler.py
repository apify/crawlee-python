import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypedDict

from fastapi import FastAPI

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


class State(TypedDict):
    """State available in the app."""

    crawler: ParselCrawler
    requests_to_results: dict[str, asyncio.Future[dict[str, str]]]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[State]:
    # Start up code that runs once when the app starts

    # Results will be stored in this dictionary
    requests_to_results = dict[str, asyncio.Future[dict[str, str]]]()

    crawler = ParselCrawler(
        # Keep the crawler alive even when there are no more requests to process now.
        # This makes the crawler wait for more requests to be added later.
        keep_alive=True
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        title = context.selector.xpath('//title/text()').get() or ''

        # Extract data from the page and save it to the result dictionary.
        requests_to_results[context.request.unique_key].set_result(
            {
                'title': title,
            }
        )

    # Start the crawler without awaiting it to finish
    crawler.log.info(f'Starting crawler for the {app.title}')
    run_task = asyncio.create_task(crawler.run([]))

    # Make the crawler and the result dictionary available in the app state
    yield {'crawler': crawler, 'requests_to_results': requests_to_results}

    # Cleanup code that runs once when the app shuts down
    crawler.stop()
    # Wait for the crawler to finish
    await run_task
