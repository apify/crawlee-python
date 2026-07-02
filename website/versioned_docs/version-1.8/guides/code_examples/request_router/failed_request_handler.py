import asyncio

from crawlee.crawlers import BasicCrawlingContext, ParselCrawler, ParselCrawlingContext


async def main() -> None:
    # Create a crawler instance with retry settings
    crawler = ParselCrawler(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
        max_request_retries=2,  # Allow 2 retries before failing
    )

    @crawler.router.default_handler
    async def default_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # Extract product information
        product_name = context.selector.css('h1[data-testid="product-title"]::text').get()
        if not product_name:
            product_name = context.selector.css('h1::text').get() or 'Unknown Product'

        price = context.selector.css('.price::text').get() or 'Price not available'

        await context.push_data(
            {
                'url': context.request.url,
                'product_name': product_name,
                'price': price,
                'status': 'success',
            }
        )

    # Failed request handler - called when request has exhausted all retries
    @crawler.failed_request_handler
    async def failed_handler(context: BasicCrawlingContext, error: Exception) -> None:
        context.log.error(
            f'Failed to process {context.request.url} after all retries: {error}'
        )

        # Save failed request information for analysis
        await context.push_data(
            {
                'failed_url': context.request.url,
                'label': context.request.label,
                'error_type': type(error).__name__,
                'error_message': str(error),
                'retry_count': context.request.retry_count,
                'status': 'failed',
            }
        )

    # Start crawling with some URLs that might fail
    await crawler.run(
        [
            'https://warehouse-theme-metal.myshopify.com/products/on-running-cloudmonster-2-mens',
            # This will likely fail
            'https://warehouse-theme-metal.myshopify.com/invalid-url',
            'https://warehouse-theme-metal.myshopify.com/products/valid-product',
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
