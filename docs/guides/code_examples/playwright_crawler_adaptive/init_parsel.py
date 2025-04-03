import asyncio

from crawlee.crawlers import AdaptivePlaywrightCrawler


async def main() -> None:
    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
        # Arguments relevant only for PlaywrightCrawler
        playwright_crawler_specific_kwargs={
            'headless': False,
            'browser_type': 'chromium',
        },
        # Common arguments relevant to all crawlers
        max_crawl_depth=5,
    )

    # ...


if __name__ == '__main__':
    asyncio.run(main())
