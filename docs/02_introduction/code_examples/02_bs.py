import asyncio

# Add import of crawler and crawling context.
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import RequestQueue


async def main() -> None:
    # First you create the request queue instance.
    rq = await RequestQueue.open()

    # And then you add one or more requests to it.
    await rq.add_request('https://crawlee.dev')

    crawler = BeautifulSoupCrawler(request_manager=rq)

    # Define a request handler and attach it to the crawler using the decorator.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        # Extract <title> text with BeautifulSoup.
        # See BeautifulSoup documentation for API docs.
        url = context.request.url
        title = context.soup.title.string if context.soup.title else ''
        context.log.info(f'The title of {url} is: {title}.')

    await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
