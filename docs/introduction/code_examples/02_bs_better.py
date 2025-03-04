import asyncio

# You don't need to import RequestQueue anymore.
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        url = context.request.url
        title = context.soup.title.string if context.soup.title else ''
        context.log.info(f'The title of {url} is: {title}.')

    # Start the crawler with the provided URLs.
    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
