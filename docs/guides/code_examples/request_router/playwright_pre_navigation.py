import asyncio

from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = PlaywrightCrawler(
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    @crawler.pre_navigation_hook
    async def setup_page(context: PlaywrightPreNavCrawlingContext) -> None:
        # Set viewport size for consistent rendering
        await context.page.set_viewport_size({'width': 1280, 'height': 720})

        # Block unnecessary resources to speed up crawling
        await context.block_requests(
            extra_url_patterns=[
                '*.png',
                '*.jpg',
                '*.jpeg',
                '*.gif',
                '*.svg',
                '*.css',
                '*.woff',
                '*.woff2',
                '*.ttf',
                '*google-analytics*',
                '*facebook*',
                '*twitter*',
            ]
        )

        # Set custom user agent
        await context.page.set_extra_http_headers(
            {
                'User-Agent': 'Mozilla/5.0 (compatible; Crawlee Bot)',
            }
        )

    @crawler.router.default_handler
    async def default_handler(context: PlaywrightCrawlingContext) -> None:
        title = await context.page.title()
        await context.push_data(
            {
                'url': context.request.url,
                'title': title,
            }
        )

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
