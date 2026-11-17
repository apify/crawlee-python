import asyncio
from datetime import timedelta
from urllib.parse import urljoin

from crawlee import Request
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.router import Router


async def main() -> None:
    router = Router[ParselCrawlingContext]()

    # Category pages list many products, so give them more time than the rest
    @router.handler('CATEGORY', request_handler_timeout=timedelta(minutes=2))
    async def category_handler(context: ParselCrawlingContext) -> None:
        context.log.info('Processing category page: %s', context.request.url)
        await context.enqueue_links(selector='a[href*="/products/"]', label='PRODUCT')

    # Product pages keep the timeout of the crawler
    @router.handler('PRODUCT')
    async def product_handler(context: ParselCrawlingContext) -> None:
        context.log.info('Processing product page: %s', context.request.url)
        image_urls = set(context.selector.css('.product-gallery img::attr(src)').getall())

        # The handler downloads every image below, and their number is known only now,
        # so ask for 5 more seconds per image
        if image_urls:
            await context.extend_timeout(timedelta(seconds=5 * len(image_urls)))

        image_sizes = []
        for image_url in image_urls:
            response = await context.send_request(urljoin(context.request.url, image_url))
            image = await response.read()
            image_sizes.append(len(image))

        await context.push_data({'url': context.request.url, 'image_sizes': image_sizes})

    crawler = ParselCrawler(
        request_handler=router,
        request_handler_timeout=timedelta(seconds=30),
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    await crawler.run(
        [
            Request.from_url(
                'https://warehouse-theme-metal.myshopify.com/collections/all',
                label='CATEGORY',
            ),
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
