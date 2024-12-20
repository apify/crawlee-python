from crawlee import EnqueueStrategy
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=50)

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}.')

        # See the EnqueueStrategy object for more strategy options.
        # highlight-next-line
        await context.enqueue_links(
            # highlight-next-line
            strategy=EnqueueStrategy.SAME_DOMAIN,
            # highlight-next-line
        )

    await crawler.run(['https://crawlee.dev/'])
