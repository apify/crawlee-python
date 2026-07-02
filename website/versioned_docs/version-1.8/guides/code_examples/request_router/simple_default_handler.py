import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    # Create a crawler instance
    crawler = ParselCrawler(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    # Use the crawler's built-in router to define a default handler
    @crawler.router.default_handler
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
        await context.enqueue_links(selector='a[href*="/products/"]', label='PRODUCT')

    # Start crawling
    await crawler.run(['https://warehouse-theme-metal.myshopify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
