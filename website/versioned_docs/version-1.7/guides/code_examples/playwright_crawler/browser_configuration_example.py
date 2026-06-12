import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler(
        headless=False,
        browser_type='chromium',
        # Browser launch options
        browser_launch_options={
            # For support `msedge` channel you need to install it
            # `playwright install msedge`
            'channel': 'msedge',
            'slow_mo': 200,
        },
        # Context launch options, applied to each page as it is created
        browser_new_context_options={
            'color_scheme': 'dark',
            # Set headers
            'extra_http_headers': {
                'Custom-Header': 'my-header',
                'Accept-Language': 'en',
            },
            # Set only User Agent
            'user_agent': 'My-User-Agent',
        },
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        await context.enqueue_links()

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
