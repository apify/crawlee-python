import asyncio
import re

from crawlee.http_clients import ImpitHttpClient
from crawlee.request_loaders import SitemapRequestLoader


async def main() -> None:
    # Create an HTTP client for fetching sitemaps
    async with ImpitHttpClient() as http_client:
        # Create a sitemap request loader with URL filtering
        sitemap_loader = SitemapRequestLoader(
            sitemap_urls=['https://crawlee.dev/sitemap.xml'],
            http_client=http_client,
            # Exclude all URLs that do not contain 'blog'
            exclude=[re.compile(r'^((?!blog).)*$')],
            max_buffer_size=500,  # Buffer up to 500 URLs in memory
        )

        # We work with the loader until we process all relevant links from the sitemap.
        while not await sitemap_loader.is_finished():
            if request := await sitemap_loader.fetch_next_request():
                # Do something with it...

                # And mark it as handled.
                await sitemap_loader.mark_request_as_handled(request)
            else:
                # If request is None, we give the loader time to get a new URL
                # from the sitemap.
                await asyncio.sleep(0.01)


if __name__ == '__main__':
    asyncio.run(main())
