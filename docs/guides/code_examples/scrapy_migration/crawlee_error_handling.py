import asyncio

from crawlee.crawlers import BasicCrawlingContext, ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(
        # Retry each failed request up to this many times (the `RetryMiddleware` analog).
        max_request_retries=3,
        max_requests_per_crawl=50,
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        for quote in context.selector.css('div.quote'):
            await context.push_data({'text': quote.css('span.text::text').get()})

    # Runs between retries. It can inspect or adjust the request before the next try.
    @crawler.error_handler
    async def on_error(context: BasicCrawlingContext, error: Exception) -> None:
        context.log.warning(f'Retrying {context.request.url}: {error}')

    # Runs once a request has exhausted all retries, like Scrapy's `errback`.
    @crawler.failed_request_handler
    async def on_failed(context: BasicCrawlingContext, error: Exception) -> None:
        context.log.error(f'Giving up on {context.request.url}: {error}')

    await crawler.run(['https://quotes.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
