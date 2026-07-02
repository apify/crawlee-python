import asyncio

from crawlee import HttpHeaders
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    @crawler.pre_navigation_hook
    async def common_setup(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        # Common pre-navigation hook - runs for both HTTP and browser requests.
        context.request.headers |= HttpHeaders(
            {'Accept': 'text/html,application/xhtml+xml'},
        )

    @crawler.pre_navigation_hook(playwright_only=True)
    async def browser_setup(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        # Playwright-specific pre-navigation hook - runs only when browser is used.
        await context.page.set_viewport_size({'width': 1280, 'height': 720})
        if context.block_requests:
            await context.block_requests(extra_url_patterns=['*.css', '*.js'])

    @crawler.router.default_handler
    async def default_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        # Extract title using the unified context interface.
        title_tag = context.parsed_content.find('title')
        title = title_tag.get_text() if title_tag else None

        # Extract other data consistently across both modes.
        links = [a.get('href') for a in context.parsed_content.find_all('a', href=True)]

        await context.push_data(
            {
                'url': context.request.url,
                'title': title,
                'links': links,
            }
        )

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
