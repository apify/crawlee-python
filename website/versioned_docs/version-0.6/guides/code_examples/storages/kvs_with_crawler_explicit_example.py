import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import KeyValueStore


async def main() -> None:
    # Open the key-value store, if it does not exist, it will be created.
    # Leave name empty to use the default KVS.
    kvs = await KeyValueStore.open()

    # Create a new Playwright crawler.
    crawler = PlaywrightCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Capture the screenshot of the page using Playwright's API.
        screenshot = await context.page.screenshot()
        name = context.request.url.split('/')[-1]

        # Store the screenshot in the key-value store.
        await kvs.set_value(
            key=f'screenshot-{name}',
            value=screenshot,
            content_type='image/png',
        )

    # Run the crawler with the initial URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
