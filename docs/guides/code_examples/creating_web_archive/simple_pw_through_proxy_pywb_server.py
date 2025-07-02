import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Use the local wayback server as a proxy
        proxy_configuration=ProxyConfiguration(proxy_urls=['http://localhost:8080/']),
        # Ignore the HTTPS errors if you have not followed pywb CA setup instructions
        browser_launch_options={'ignore_https_errors': True},
        max_requests_per_crawl=10,
        headless=False,
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Archiving {context.request.url} ...')
        # For some sites, where the content loads dynamically,
        # it is needed to scroll the page to load all content.
        # It slows down the crawling, but ensures that all content is loaded.
        await context.infinite_scroll()
        await context.enqueue_links(strategy='same-domain')

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
