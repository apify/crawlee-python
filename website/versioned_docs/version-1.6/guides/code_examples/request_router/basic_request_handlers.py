import asyncio

from crawlee import Request
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.router import Router


async def main() -> None:
    # Create a custom router instance
    router = Router[ParselCrawlingContext]()

    # Define the default handler (fallback for requests without specific labels)
    @router.default_handler
    async def default_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing homepage: {context.request.url}')

        # Extract page title
        title = context.selector.css('title::text').get() or 'No title found'

        await context.push_data(
            {
                'url': context.request.url,
                'title': title,
                'page_type': 'homepage',
            }
        )

        # Find and enqueue collection/category links
        await context.enqueue_links(selector='a[href*="/collections/"]', label='CATEGORY')

    # Define a handler for category pages
    @router.handler('CATEGORY')
    async def category_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing category page: {context.request.url}')

        # Extract category information
        category_title = context.selector.css('h1::text').get() or 'Unknown Category'
        product_count = len(context.selector.css('.product-item').getall())

        await context.push_data(
            {
                'url': context.request.url,
                'type': 'category',
                'category_title': category_title,
                'product_count': product_count,
                'handler': 'category',
            }
        )

        # Enqueue product links from this category
        await context.enqueue_links(selector='a[href*="/products/"]', label='PRODUCT')

    # Define a handler for product detail pages
    @router.handler('PRODUCT')
    async def product_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing product page: {context.request.url}')

        # Extract detailed product information
        product_data = {
            'url': context.request.url,
            'name': context.selector.css('h1::text').get(),
            'price': context.selector.css('.price::text').get(),
            'description': context.selector.css('.product-description p::text').get(),
            'images': context.selector.css('.product-gallery img::attr(src)').getall(),
            'in_stock': bool(context.selector.css('.add-to-cart-button').get()),
            'handler': 'product',
        }

        await context.push_data(product_data)

    # Create crawler with the router
    crawler = ParselCrawler(
        request_handler=router,
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    # Start crawling with some initial requests
    await crawler.run(
        [
            # Will use default handler
            'https://warehouse-theme-metal.myshopify.com/',
            # Will use category handler
            Request.from_url(
                'https://warehouse-theme-metal.myshopify.com/collections/all',
                label='CATEGORY',
            ),
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
