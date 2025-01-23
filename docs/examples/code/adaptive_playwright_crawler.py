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

    @crawler.router.handler(label='label')
    async def request_handler_for_label(context: AdaptivePlaywrightCrawlingContext) -> None:
        # Do some processing using `page`
        some_locator = context.page.locator('div').first
        await some_locator.wait_for()
        # Do stuff with locator...
        context.log.info(f'Playwright processing of: {context.request.url} ...')

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        context.log.info(f'User handler processing: {context.request.url} ...')
        # Do some processing using `parsed_content`
        context.log.info(context.parsed_content.title)

        # Find more links and enqueue them.
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
