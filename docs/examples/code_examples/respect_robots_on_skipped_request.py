import asyncio

from crawlee import SkippedReason
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

    # highlight-start
    # This handler is called when a request is skipped
    @crawler.on_skipped_request
    async def skipped_request_handler(url: str, reason: SkippedReason) -> None:
        # Check if the request was skipped due to robots.txt rules
        if reason == 'robots_txt':
            crawler.log.info(f'Skipped {url} due to robots.txt rules.')

    # highlight-end

    # Start the crawler with the specified URLs
    # The login URL will be skipped and handled by the skipped_request_handler
    await crawler.run(
        ['https://news.ycombinator.com/', 'https://news.ycombinator.com/login']
    )


if __name__ == '__main__':
    asyncio.run(main())
