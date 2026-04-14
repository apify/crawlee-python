import asyncio

from crawlee import HttpHeaders
from crawlee.crawlers import BasicCrawlingContext, ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    @crawler.pre_navigation_hook
    async def setup_request(context: BasicCrawlingContext) -> None:
        # Add custom headers before making the request
        context.request.headers |= HttpHeaders(
            {
                'User-Agent': 'Crawlee Bot 1.0',
                'Accept': 'text/html,application/xhtml+xml',
            },
        )

    @crawler.router.default_handler
    async def default_handler(context: ParselCrawlingContext) -> None:
        # Extract basic page information
        title = context.selector.css('title::text').get()
        await context.push_data(
            {
                'url': context.request.url,
                'title': title,
            }
        )

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
