import asyncio
import json
from datetime import timedelta
from typing import Any

from aws_lambda_powertools.utilities.typing import LambdaContext

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
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

    crawler = PlaywrightCrawler(
        storage_client=storage_client,
        max_request_retries=1,
        request_handler_timeout=timedelta(seconds=30),
        max_requests_per_crawl=10,
        # highlight-start
        # Configure Playwright to run in AWS Lambda environment
        browser_launch_options={
            'args': [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--single-process',
            ]
        },
        # highlight-end
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        data = {
            'url': context.request.url,
            'title': await context.page.title(),
            'h1s': await context.page.locator('h1').all_text_contents(),
            'h2s': await context.page.locator('h2').all_text_contents(),
            'h3s': await context.page.locator('h3').all_text_contents(),
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
