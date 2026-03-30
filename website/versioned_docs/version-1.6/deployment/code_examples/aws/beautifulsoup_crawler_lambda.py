import asyncio
import json
from datetime import timedelta
from typing import Any

from aws_lambda_powertools.utilities.typing import LambdaContext

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset, RequestQueue


async def main() -> str:
    # highlight-start
    # Disable writing storage data to the file system
    storage_client = MemoryStorageClient()
    # highlight-end

    # Initialize storages
    dataset = await Dataset.open(storage_client=storage_client)
    request_queue = await RequestQueue.open(storage_client=storage_client)

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

    # Extract data saved in `Dataset`
    data = await crawler.get_data()

    # Clean up storages after the crawl
    await dataset.drop()
    await request_queue.drop()

    # Serialize the list of scraped items to JSON string
    return json.dumps(data.items)


def lambda_handler(_event: dict[str, Any], _context: LambdaContext) -> dict[str, Any]:
    result = asyncio.run(main())
    # Return the response with results
    return {'statusCode': 200, 'body': result}
