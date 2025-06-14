# mypy: disable-error-code="misc"
import asyncio
import json
from datetime import timedelta

import functions_framework
from flask import Request, Response

from crawlee.crawlers import (
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
)
from crawlee.storage_clients import MemoryStorageClient


async def main() -> str:
    # highlight-start
    # Disable writing storage data to the file system
    storage_client = MemoryStorageClient()
    # highlight-end

    crawler = BeautifulSoupCrawler(
        storage_client=storage_client,
        max_request_retries=1,
        request_handler_timeout=timedelta(seconds=30),
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
            'h1s': [h1.text for h1 in context.soup.find_all('h1')],
            'h2s': [h2.text for h2 in context.soup.find_all('h2')],
            'h3s': [h3.text for h3 in context.soup.find_all('h3')],
        }

        await context.push_data(data)
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev'])

    # highlight-start
    # Extract data saved in `Dataset`
    data = await crawler.get_data()
    # Serialize to json string and return
    return json.dumps(data.items)
    # highlight-end


@functions_framework.http
def crawlee_run(request: Request) -> Response:
    # You can pass data to your crawler using `request`
    function_id = request.headers['Function-Execution-Id']
    response_str = asyncio.run(main())

    # Return a response with the crawling results
    return Response(response=response_str, status=200)
