from crawlee import ExtractStrategy
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=50)

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}.')

        # See the ExtractStrategy object for more strategy options.
        # highlight-next-line
        await context.extract_links(
            # highlight-next-line
            strategy=ExtractStrategy.SAME_DOMAIN,
            # highlight-next-line
        )

    await crawler.run(['https://crawlee.dev/'])
