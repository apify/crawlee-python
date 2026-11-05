import asyncio
import logging

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    # Each crawler logs to its own logger, so you can tell their messages apart.
    products_crawler = ParselCrawler(logger=logging.getLogger('shop.products'))
    reviews_crawler = ParselCrawler(logger=logging.getLogger('shop.reviews'))

    @products_crawler.router.default_handler
    async def products_handler(context: ParselCrawlingContext) -> None:
        # The `context.log` is the crawler's logger, `shop.products`.
        context.log.info('Product page %s', context.request.url)

    @reviews_crawler.router.default_handler
    async def reviews_handler(context: ParselCrawlingContext) -> None:
        context.log.info('Review page %s', context.request.url)

    await asyncio.gather(
        products_crawler.run(['https://crawlee.dev/']),
        reviews_crawler.run(['https://crawlee.dev/python/']),
    )


if __name__ == '__main__':
    asyncio.run(main())
