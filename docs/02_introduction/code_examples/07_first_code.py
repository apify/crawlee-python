import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import Dataset

# ...


async def main() -> None:
    crawler = PlaywrightCrawler()
    dataset = await Dataset.open()

    # ...

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        ...
        # ...


if __name__ == '__main__':
    asyncio.run(main())
