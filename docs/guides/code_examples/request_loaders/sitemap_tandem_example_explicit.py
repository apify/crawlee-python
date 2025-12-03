import asyncio
import re

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.http_clients import ImpitHttpClient
from crawlee.request_loaders import RequestManagerTandem, SitemapRequestLoader
from crawlee.storages import RequestQueue


async def main() -> None:
    # Create an HTTP client for fetching the sitemap.
    http_client = ImpitHttpClient()

    # Create a sitemap request loader with filtering rules.
    sitemap_loader = SitemapRequestLoader(
        sitemap_urls=['https://crawlee.dev/sitemap.xml'],
        http_client=http_client,
        include=[re.compile(r'.*docs.*')],  # Only include URLs containing 'docs'.
        max_buffer_size=500,  # Keep up to 500 URLs in memory before processing.
    )

    # Open the default request queue.
    request_queue = await RequestQueue.open()

    # And combine them together to a single request manager.
    request_manager = RequestManagerTandem(sitemap_loader, request_queue)

    # Create a crawler and pass the request manager to it.
    crawler = ParselCrawler(
        request_manager=request_manager,
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # New links will be enqueued directly to the queue.
        await context.enqueue_links()

        # Extract data using Parsel's XPath and CSS selectors.
        data = {
            'url': context.request.url,
            'title': context.selector.xpath('//title/text()').get(),
        }

        # Push extracted data to the dataset.
        await context.push_data(data)

    await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
