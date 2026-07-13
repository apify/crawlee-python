import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    # highlight-start
    # `PlaywrightCrawler` renders JavaScript in a real browser. It replaces the
    # `scrapy-playwright` package, with browser support built into the framework.
    crawler = PlaywrightCrawler(
        headless=True,
        max_requests_per_crawl=50,
    )
    # highlight-end

    @crawler.router.default_handler
    async def handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # highlight-start
        # `context.page` is a Playwright `Page`. Query the rendered DOM directly.
        for quote in await context.page.locator('div.quote').all():
            await context.push_data(
                {
                    'text': await quote.locator('span.text').text_content(),
                    'author': await quote.locator('small.author').text_content(),
                }
            )
        # highlight-end

        await context.enqueue_links(selector='li.next a')

    await crawler.run(['https://quotes.toscrape.com/js/'])


if __name__ == '__main__':
    asyncio.run(main())
