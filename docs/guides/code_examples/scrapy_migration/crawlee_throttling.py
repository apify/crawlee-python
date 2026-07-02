import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.request_loaders import ThrottlingRequestManager
from crawlee.storages import RequestQueue


async def main() -> None:
    # A regular request queue holds requests for non-throttled domains.
    request_queue = await RequestQueue.open()

    # `ThrottlingRequestManager` wraps the queue and adds per-domain backoff.
    # It reacts to HTTP 429 responses and `robots.txt` crawl-delay directives, which
    # makes it the closest built-in analog to Scrapy's `AutoThrottle`. The crawler
    # feeds it those signals, so you only list the domains to watch.
    request_manager = ThrottlingRequestManager(
        inner=request_queue,
        domains=['quotes.toscrape.com'],
        request_manager_opener=RequestQueue.open,
    )

    crawler = ParselCrawler(
        request_manager=request_manager,
        # Crawl-delay is only read when `robots.txt` handling is enabled.
        respect_robots_txt_file=True,
        max_requests_per_crawl=50,
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')
        await context.enqueue_links(selector='li.next a')

    await crawler.run(['https://quotes.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
