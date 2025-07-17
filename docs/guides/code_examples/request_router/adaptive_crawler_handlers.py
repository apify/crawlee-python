import asyncio

from crawlee import HttpHeaders
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptiveContextError,
)


async def main() -> None:
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    # Common pre-navigation hook (runs for all requests)
    @crawler.pre_navigation_hook
    async def common_setup(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        # This runs for both HTTP and browser requests
        context.request.headers |= HttpHeaders(
            {'Accept': 'text/html,application/xhtml+xml'},
        )

    # Playwright-specific pre-navigation hook (only when using browser)
    @crawler.pre_navigation_hook(playwright_only=True)
    async def browser_setup(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        # This only runs when browser is used
        await context.page.set_viewport_size({'width': 1280, 'height': 720})
        if context.block_requests:
            await context.block_requests(extra_url_patterns=['*.css', '*.js'])

    @crawler.router.default_handler
    async def default_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # Try browser-based extraction first
            page = context.page
            title = await page.title()
            method = 'browser'
        except AdaptiveContextError:
            # Fallback to static parsing
            title_tag = context.parsed_content.find('title')
            title = title_tag.get_text() if title_tag else 'No title'
            method = 'static'

        await context.push_data(
            {
                'url': context.request.url,
                'title': title,
                'method': method,
            }
        )

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
