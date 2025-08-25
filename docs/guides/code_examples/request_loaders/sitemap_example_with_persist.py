import asyncio
import logging

from crawlee import service_locator
from crawlee.http_clients import ImpitHttpClient
from crawlee.request_loaders import SitemapRequestLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)


# Disable clearing the `KeyValueStore` on each run.
# This is necessary so that the state keys are not cleared at startup.
# The recommended way to achieve this behavior is setting the environment variable
# `CRAWLEE_PURGE_ON_START=0`
configuration = service_locator.get_configuration()
configuration.purge_on_start = False


async def main() -> None:
    # Create an HTTP client for fetching sitemaps
    # Use the context manager for `SitemapRequestLoader` to correctly save the state when
    # the work is completed.
    async with (
        ImpitHttpClient() as http_client,
        SitemapRequestLoader(
            sitemap_urls=['https://crawlee.dev/sitemap.xml'],
            http_client=http_client,
            # Enable persistence
            persist_state_key='my-persist-state',
        ) as sitemap_loader,
    ):
        # We receive only one request.
        # Each time you run it, it will be a new request until you exhaust the sitemap.
        request = await sitemap_loader.fetch_next_request()
        if request:
            logger.info(f'Processing request: {request.url}')
            # Do something with it...

            # And mark it as handled.
            await sitemap_loader.mark_request_as_handled(request)


if __name__ == '__main__':
    asyncio.run(main())
