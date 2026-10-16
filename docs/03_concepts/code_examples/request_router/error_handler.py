import asyncio

from crawlee.crawlers import BasicCrawlingContext, ParselCrawler, ParselCrawlingContext
from crawlee.errors import HttpStatusCodeError

# HTTP status code constants
TOO_MANY_REQUESTS = 429


async def main() -> None:
    # Create a crawler instance
    crawler = ParselCrawler(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    @crawler.router.default_handler
    async def default_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # Extract product information (might fail for some pages)
        product_name = context.selector.css('h1[data-testid="product-title"]::text').get()
        if not product_name:
            raise ValueError('Product name not found - might be a non-product page')

        price = context.selector.css('.price::text').get()
        await context.push_data(
            {
                'url': context.request.url,
                'product_name': product_name,
                'price': price,
            }
        )

    # Error handler - called when an error occurs during request processing
    @crawler.error_handler
    async def error_handler(context: BasicCrawlingContext, error: Exception) -> None:
        error_name = type(error).__name__
        context.log.warning(f'Error occurred for {context.request.url}: {error_name}')

        # You can modify the request or context here before retry
        if (
            isinstance(error, HttpStatusCodeError)
            and error.status_code == TOO_MANY_REQUESTS
        ):
            context.log.info('Rate limited - will retry with delay')
            # You could modify headers, add delay, etc.
        elif isinstance(error, ValueError):
            context.log.info('Parse error - marking request as no retry')
            context.request.no_retry = True

    # Start crawling
    await crawler.run(
        [
            'https://warehouse-theme-metal.myshopify.com/products/on-running-cloudmonster-2-mens',
            # Might cause parse error
            'https://warehouse-theme-metal.myshopify.com/collections/mens-running',
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
