import asyncio

from crawlee.playwright_crawler.playwright_crawler import PlaywrightCrawler

from .routes import router


async def main() -> None:
    """The crawler entry point."""
    crawler = PlaywrightCrawler(
        request_handler=router,
        max_requests_per_crawl=50,
    )

    await crawler.run(
        [
            'https://crawlee.dev',
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
