import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    # PlaywrightCrawler crawls the web using a headless browser
    # controlled by the Playwright library.
    crawler = PlaywrightCrawler()

    # Define a request handler to process each crawled page
    # and attach it to the crawler using a decorator.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        # Extract relevant data from the page context.
        data = {
            'url': context.request.url,
            'title': await context.page.title(),
        }
        # Store the extracted data.
        await context.push_data(data)
        # Extract links from the current page and add them to the crawling queue.
        await context.enqueue_links()

    # Add first URL to the queue and start the crawl.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
