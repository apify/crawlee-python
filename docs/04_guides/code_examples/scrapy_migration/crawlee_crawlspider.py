import asyncio

from crawlee import Glob
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=50)

    # The default handler plays the role of CrawlSpider's `rules`. It follows the
    # pagination and enqueues each book detail page, routed by label.
    @crawler.router.default_handler
    async def listing_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Listing {context.request.url}')

        # highlight-start
        # `selector` is the `restrict_css` analog. `include` is the `allow` analog:
        # it keeps only URLs matching the given globs.
        await context.enqueue_links(
            selector='article.product_pod h3 a',
            include=[Glob('https://books.toscrape.com/catalogue/**')],
            label='book',
        )
        # highlight-end

        # Follow pagination without a label, like a `Rule` with no callback.
        await context.enqueue_links(selector='li.next a')

    # Routed by the 'book' label, like a `Rule` with `callback='parse_book'`.
    @crawler.router.handler('book')
    async def book_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Book {context.request.url}')
        await context.push_data(
            {
                'title': context.selector.css('h1::text').get(),
                'price': context.selector.css('p.price_color::text').get(),
            }
        )

    await crawler.run(['https://books.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
