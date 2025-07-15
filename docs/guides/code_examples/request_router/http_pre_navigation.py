import asyncio

from crawlee.crawlers import BasicCrawlingContext, ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler()

    @crawler.pre_navigation_hook
    async def setup_request(context: BasicCrawlingContext) -> None:
        # Add custom headers before making the request
        context.request.headers['User-Agent'] = 'Crawlee Bot 1.0'
        context.request.headers['Accept'] = 'text/html,application/xhtml+xml'

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
