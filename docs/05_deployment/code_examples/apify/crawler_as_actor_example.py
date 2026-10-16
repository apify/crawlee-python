import asyncio

from apify import Actor

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    # Wrap the crawler code in an Actor context manager.
    async with Actor:
        crawler = BeautifulSoupCrawler(max_requests_per_crawl=10)

        @crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url} ...')
            data = {
                'url': context.request.url,
                'title': context.soup.title.string if context.soup.title else None,
            }
            await context.push_data(data)
            await context.enqueue_links()

        await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
