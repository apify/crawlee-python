import asyncio

# highlight-next-line
from apify import Actor

from crawlee.crawlers import PlaywrightCrawler

from .routes import router


async def main() -> None:
    # highlight-next-line
    async with Actor:
        crawler = PlaywrightCrawler(
            # Let's limit our crawls to make our tests shorter and safer.
            max_requests_per_crawl=50,
            # Provide our router instance to the crawler.
            request_handler=router,
        )

        await crawler.run(['https://warehouse-theme-metal.myshopify.com/collections'])


if __name__ == '__main__':
    asyncio.run(main())
