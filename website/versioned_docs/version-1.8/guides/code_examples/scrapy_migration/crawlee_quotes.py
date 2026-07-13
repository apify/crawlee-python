import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=50)

    # The default handler runs for the entry point and every paginated page.
    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # `context.selector` is a Parsel `Selector`, the same object Scrapy exposes
        # as `response`. CSS and XPath queries carry over unchanged.
        items = [
            {
                'text': quote.css('span.text::text').get(),
                'author': quote.css('small.author::text').get(),
                'tags': quote.css('div.tags a.tag::text').getall(),
            }
            for quote in context.selector.css('div.quote')
        ]
        await context.push_data(items)

        # Follow the pagination link to the next page.
        await context.enqueue_links(selector='li.next a')

    await crawler.run(['https://quotes.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
