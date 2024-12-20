import asyncio

from crawlee.adaptive_crawler._adaptive_playwright_crawler import AdaptivePlayWrightCrawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler



async def main():

    adaptive_crawler = await AdaptivePlayWrightCrawler.create_with_default_settings()

    @adaptive_crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawler) -> None:
        context.log.info(f'Processing with Top adaptive_crawler: {context.request.url} ...')
        await context.push_data({"Top crwaler Url": context.request.url})

    await adaptive_crawler.run(['https://crawlee.dev',
                                "https://crawlee.dev/docs/quick-start",
                                "https://crawlee.dev/docs/examples",
                                "https://crawlee.dev/api/core"])


if __name__ == '__main__':
    asyncio.run(main())
