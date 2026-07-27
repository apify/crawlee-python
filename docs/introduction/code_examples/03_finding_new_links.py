import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    # Let's limit our crawls to make our tests shorter and safer.
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=20)

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        url = context.request.url
        title = context.soup.title.string if context.soup.title else ''
        context.log.info(f'The title of {url} is: {title}.')

        # The enqueue_links function is available as one of the fields of the context.
        # It is also context aware, so it does not require any parameters.
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
