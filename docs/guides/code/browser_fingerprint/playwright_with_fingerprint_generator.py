import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.fingerprint_suite import DefaultFingerprintGenerator


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Fingerprint generator to be used. By default no fingerprint generation is done.
        fingerprint_generator=DefaultFingerprintGenerator(),
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Find a link to the next page and enqueue it if it exists.
        await context.enqueue_links(selector='.morelink')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://news.ycombinator.com/'])


if __name__ == '__main__':
    asyncio.run(main())
