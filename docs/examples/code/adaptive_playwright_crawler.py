import asyncio
from datetime import timedelta

from playwright.async_api import Route

from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    # Crawler created by following factory method will use `beautifulsoup`
    # for parsing static content.
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_requests_per_crawl=5, playwright_crawler_specific_kwargs={'headless': False}
    )

    @crawler.router.default_handler
    async def request_handler_for_label(
        context: AdaptivePlaywrightCrawlingContext,
    ) -> None:
        # Do some processing using `parsed_content`
        context.log.info(context.parsed_content.title)

        # Locate element h2 within 5 seconds
        h2 = await context.query_selector_one('h2', timedelta(milliseconds=5000))
        # Do stuff with element found by the selector
        context.log.info(h2)

        # Find more links and enqueue them.
        await context.enqueue_links()
        # Save some data.
        await context.push_data({'Visited url': context.request.url})

    @crawler.pre_navigation_hook
    async def hook(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        """Hook executed both in static sub crawler and playwright sub crawler.

        Trying to access `context.page` in this hook would raise `AdaptiveContextError`
        for pages crawled without playwright."""
        context.log.info(f'pre navigation hook for: {context.request.url} ...')

    @crawler.pre_navigation_hook(playwright_only=True)
    async def hook_playwright(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        """Hook executed only in playwright sub crawler.

        It is safe to access `page` object.
        """

        async def some_routing_function(route: Route) -> None:
            await route.continue_()

        await context.page.route('*/**', some_routing_function)
        context.log.info(
            f'Playwright only pre navigation hook for: {context.request.url} ...'
        )

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://warehouse-theme-metal.myshopify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
