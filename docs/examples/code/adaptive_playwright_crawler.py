import asyncio

from playwright.async_api import Route

from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler import AdaptivePlaywrightCrawler
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptiveContextError,
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_requests_per_crawl=5, playwright_crawler_specific_kwargs={'headless': False}
    )

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        # Code that will be executed in both crawl types
        context.log.info(f'User handler processing: {context.request.url} ...')

        try:
            some_locator = context.page.locator('div').first
            # Code that will be executed only in Playwright crawl.
            # Trying to access `context.page` in static crawl will throw `AdaptiveContextError`.

            await some_locator.wait_for()
            # Do stuff with locator...
            context.log.info(f'Playwright processing of: {context.request.url} ...')
        except AdaptiveContextError:
            # Code that will be executed in only in static crawl
            context.log.info(f'Static processing of: {context.request.url} ...')

        # FInd more links and enqueue them.
        await context.enqueue_links()
        await context.push_data({'Top crawler Url': context.request.url})

    @crawler.pre_navigation_hook
    async def hook(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        async def some_routing_function(route: Route) -> None:
            await route.continue_()

        try:
            await context.page.route('*/**', some_routing_function)
            context.log.info(f'Playwright pre navigation hook for: {context.request.url} ...')
        except AdaptiveContextError:
            context.log.info(f'Static pre navigation hook for: {context.request.url} ...')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://warehouse-theme-metal.myshopify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
