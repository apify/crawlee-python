import asyncio
import re

from crawlee.http_clients import ImpitHttpClient
from crawlee.request_loaders import SitemapRequestLoader


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

    # We work with the loader until we process all relevant links from the sitemap.
    while request := await sitemap_loader.fetch_next_request():
        # Do something with it...
        print(f'Processing {request.url}')

        # And mark it as handled.
        await sitemap_loader.mark_request_as_handled(request)


if __name__ == '__main__':
    asyncio.run(main())
