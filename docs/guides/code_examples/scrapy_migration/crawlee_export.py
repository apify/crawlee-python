import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=50)

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        items = [
            {
                'text': quote.css('span.text::text').get(),
                'author': quote.css('small.author::text').get(),
            }
            for quote in context.selector.css('div.quote')
        ]
        await context.push_data(items)

        await context.enqueue_links(selector='li.next a')

    await crawler.run(['https://quotes.toscrape.com/'])

    # highlight-start
    # Export the whole dataset to a file. The format follows the extension,
    # which must be .json or .csv. It replaces Scrapy's `FEEDS` setting and
    # the `-O output.json` CLI flag.
    await crawler.export_data('quotes.json')
    await crawler.export_data('quotes.csv')
    # highlight-end


if __name__ == '__main__':
    asyncio.run(main())
