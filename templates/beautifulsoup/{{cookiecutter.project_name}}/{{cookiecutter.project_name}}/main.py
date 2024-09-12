from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler

from .routes import router


async def main() -> None:
    """The crawler entry point."""
    crawler = BeautifulSoupCrawler(
        request_handler=router,
        max_requests_per_crawl=50,
    )

    await crawler.run(
        [
            'https://crawlee.dev',
        ]
    )
