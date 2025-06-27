import asyncio
import re

from crawlee.http_clients import HttpxHttpClient
from crawlee.request_loaders import SitemapRequestLoader


async def main() -> None:
    # Create an HTTP client for fetching sitemaps
    async with HttpxHttpClient() as http_client:
        # Create a sitemap request loader with URL filtering
        sitemap_loader = SitemapRequestLoader(
            sitemap_urls=['https://crawlee.dev/sitemap.xml'],
            http_client=http_client,
            # Exclude all URLs that do not contain 'blog'
            exclude=[re.compile(r'^((?!blog).)*$')],
            max_buffer_size=500,  # Buffer up to 500 URLs in memory
        )

        while request := await sitemap_loader.fetch_next_request():
            # Do something with it...

            # And mark it as handled.
            await sitemap_loader.mark_request_as_handled(request)


if __name__ == '__main__':
    asyncio.run(main())
