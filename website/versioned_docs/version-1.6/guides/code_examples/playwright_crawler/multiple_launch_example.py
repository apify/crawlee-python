import asyncio

from crawlee.browsers import BrowserPool, PlaywrightBrowserPlugin
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    # Create a plugin for each required browser.
    plugin_chromium = PlaywrightBrowserPlugin(
        browser_type='chromium', max_open_pages_per_browser=1
    )
    plugin_firefox = PlaywrightBrowserPlugin(
        browser_type='firefox', max_open_pages_per_browser=1
    )

    crawler = PlaywrightCrawler(
        browser_pool=BrowserPool(plugins=[plugin_chromium, plugin_firefox]),
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        browser_name = (
            context.page.context.browser.browser_type.name
            if context.page.context.browser
            else 'undefined'
        )
        context.log.info(f'Processing {context.request.url} with {browser_name} ...')

        await context.enqueue_links()

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev', 'https://apify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
