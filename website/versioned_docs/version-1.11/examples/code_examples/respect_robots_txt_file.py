import asyncio

from crawlee.crawlers import (
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
)


async def main() -> None:
    # Initialize the crawler with robots.txt compliance enabled
    crawler = BeautifulSoupCrawler(respect_robots_txt_file=True)

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # Start the crawler with the specified URLs
    # The crawler will check the robots.txt file before making requests
    # In this example, 'https://news.ycombinator.com/login' will be skipped
    # because it's disallowed in the site's robots.txt file
    await crawler.run(
        ['https://news.ycombinator.com/', 'https://news.ycombinator.com/login']
    )


if __name__ == '__main__':
    asyncio.run(main())
