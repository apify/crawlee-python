import asyncio

from playwright.async_api import Route

from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser()

    @crawler.pre_navigation_hook
    async def hook(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        """Hook executed both in static sub crawler and playwright sub crawler.

        Trying to access `context.page` in this hook would raise `AdaptiveContextError`
        for pages crawled without playwright.
        """
        context.log.info(f'pre navigation hook for: {context.request.url}')

    @crawler.pre_navigation_hook(playwright_only=True)
    async def hook_playwright(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        """Hook executed only in playwright sub crawler."""

        async def some_routing_function(route: Route) -> None:
            await route.continue_()

        await context.page.route('*/**', some_routing_function)
        context.log.info(
            f'Playwright only pre navigation hook for: {context.request.url}'
        )

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
