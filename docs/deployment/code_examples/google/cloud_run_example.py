# mypy: disable-error-code="misc"
import json
import os

import uvicorn
from litestar import Litestar, get

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storage_clients import MemoryStorageClient


@get('/')
async def main() -> str:
    """The crawler entry point that will be called when the HTTP endpoint is accessed."""
    # highlight-start
    # Disable writing storage data to the file system
    storage_client = MemoryStorageClient()
    # highlight-end

    crawler = PlaywrightCrawler(
        headless=True,
        max_requests_per_crawl=10,
        browser_type='firefox',
        storage_client=storage_client,
    )

    @crawler.router.default_handler
    async def default_handler(context: PlaywrightCrawlingContext) -> None:
        """Default request handler that processes each page during crawling."""
        context.log.info(f'Processing {context.request.url} ...')
        title = await context.page.query_selector('title')
        await context.push_data(
            {
                'url': context.request.loaded_url,
                'title': await title.inner_text() if title else None,
            }
        )

        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev'])

    data = await crawler.get_data()

    # Return the results as JSON to the client
    return json.dumps(data.items)


# Initialize the Litestar app with our route handler
app = Litestar(route_handlers=[main])

# Start the Uvicorn server using the `PORT` environment variable provided by GCP
# This is crucial - Cloud Run expects your app to listen on this specific port
uvicorn.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', '8080')))  # noqa: S104 # Use all interfaces in a container, safely
