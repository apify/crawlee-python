import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.router import Router


async def main() -> None:
    # Create a custom router instance
    router = Router[ParselCrawlingContext]()

    # Define only a default handler
    @router.default_handler
    async def default_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # Extract page title
        title = context.selector.css('title::text').get() or 'No title found'

        # Extract and save basic page data
        await context.push_data(
            {
                'url': context.request.url,
                'title': title,
            }
        )

        # Find and enqueue product links for further crawling
        await context.enqueue_links(
            selector='a[href*="/products/"]',
            label='PRODUCT',  # Note: no handler for this label, will use default
        )

    # Create crawler with the custom router
    crawler = ParselCrawler(
        request_handler=router,
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    # Start crawling
    await crawler.run(['https://warehouse-theme-metal.myshopify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
