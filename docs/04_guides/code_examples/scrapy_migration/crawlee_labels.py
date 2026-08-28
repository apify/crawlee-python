import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=50)

    # The default handler processes listing pages: the entry point and each
    # paginated page. It routes author links to a separate handler by label.
    @crawler.router.default_handler
    async def listing_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Listing {context.request.url}')

        # highlight-start
        # Enqueue author detail pages with a label. It replaces a Scrapy
        # `Request(url, callback=self.parse_author)`.
        await context.enqueue_links(selector='div.quote span a', label='author')
        # highlight-end

        # Follow the pagination link.
        await context.enqueue_links(selector='li.next a')

    # This handler runs only for requests labeled 'author'.
    # highlight-next-line
    @crawler.router.handler('author')
    async def author_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Author {context.request.url}')

        await context.push_data(
            {
                'name': context.selector.css('h3.author-title::text').get(),
                'born': context.selector.css('span.author-born-date::text').get(),
                'bio': context.selector.css('div.author-description::text').get(),
            }
        )

    await crawler.run(['https://quotes.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
