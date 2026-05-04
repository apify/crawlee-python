import asyncio

from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightCrawlingContext,
)

from .selectolax_parser import SelectolaxLexborParser


async def main() -> None:
    crawler: AdaptivePlaywrightCrawler = AdaptivePlaywrightCrawler(
        max_requests_per_crawl=10,
        # Use custom Selectolax parser for static content parsing.
        static_parser=SelectolaxLexborParser(),
    )

    @crawler.router.default_handler
    async def handle_request(context: AdaptivePlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        data = {
            'url': context.request.url,
            'title': await context.query_selector_one('title'),
        }

        await context.push_data(data)

        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
