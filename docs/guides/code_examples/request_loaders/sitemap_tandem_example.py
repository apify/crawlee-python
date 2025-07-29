import asyncio
import re

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.http_clients import HttpxHttpClient
from crawlee.request_loaders import SitemapRequestLoader


async def main() -> None:
    # Create an HTTP client for fetching sitemaps
    async with HttpxHttpClient() as http_client:
        # Create a sitemap request loader with URL filtering
        sitemap_loader = SitemapRequestLoader(
            sitemap_urls=['https://crawlee.dev/sitemap.xml'],
            http_client=http_client,
            # Include only URLs that contain 'docs'
            include=[re.compile(r'.*docs.*')],
            max_buffer_size=500,  # Buffer up to 500 URLs in memory
        )

        # Convert the sitemap loader to a request manager using the to_tandem method.
        # It is a tandem with the default request queue.
        request_manager = await sitemap_loader.to_tandem()

        # Create a crawler and pass the request manager to it.
        crawler = ParselCrawler(
            request_manager=request_manager,
            max_requests_per_crawl=10,  # Limit the max requests per crawl.
        )

        @crawler.router.default_handler
        async def handler(context: ParselCrawlingContext) -> None:
            # New links will be enqueued directly to the queue.
            await context.enqueue_links()

        await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
