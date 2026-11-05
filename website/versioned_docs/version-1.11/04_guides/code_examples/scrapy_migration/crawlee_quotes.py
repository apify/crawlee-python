import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=50)

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        await context.push_data(
            [
                {
                    'text': quote.css('span.text::text').get(),
                    'author': quote.css('small.author::text').get(),
                    'tags': quote.css('div.tags a.tag::text').getall(),
                }
                for quote in context.selector.css('div.quote')
            ]
        )
        await context.enqueue_links(selector='li.next a')

    await crawler.run(['https://quotes.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
