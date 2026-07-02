import asyncio
from urllib.parse import urljoin

from crawlee import Request
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=50)

    @crawler.router.default_handler
    async def listing_handler(context: ParselCrawlingContext) -> None:
        requests: list[Request] = []

        for book in context.selector.css('article.product_pod'):
            href = book.css('h3 a::attr(href)').get()
            if href is None:
                continue

            # `user_data` travels with the request to the detail handler,
            # the same role Scrapy's `cb_kwargs` plays.
            requests.append(
                Request.from_url(
                    urljoin(context.request.url, href),
                    label='book',
                    user_data={
                        'listing_price': book.css('p.price_color::text').get(),
                    },
                )
            )

        await context.add_requests(requests)

    @crawler.router.handler('book')
    async def book_handler(context: ParselCrawlingContext) -> None:
        await context.push_data(
            {
                'title': context.selector.css('h1::text').get(),
                'listing_price': context.request.user_data.get('listing_price'),
            }
        )

    await crawler.run(['https://books.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
