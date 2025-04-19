# mypy: disable-error-code="misc"
import json
import os

import uvicorn
from litestar import Litestar, get

from crawlee import service_locator
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.http_clients import HttpxHttpClient

# highlight-start
# Disable writing storage data to the file system
configuration = service_locator.get_configuration()
configuration.persist_storage = False
configuration.write_metadata = False
# highlight-end


@get('/')
async def main() -> str:
    """The crawler entry point."""
    crawler = PlaywrightCrawler(
        headless=True,
        max_requests_per_crawl=10,
        http_client=HttpxHttpClient(),
        browser_type='firefox',
    )

    @crawler.router.default_handler
    async def default_handler(context: PlaywrightCrawlingContext) -> None:
        """Default request handler."""
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

    return json.dumps(data.items)


app = Litestar(route_handlers=[main])
uvicorn.run(app, host='127.0.0.1', port=int(os.environ.get('PORT', '8080')))
