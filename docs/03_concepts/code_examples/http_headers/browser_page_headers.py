import asyncio

from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = PlaywrightCrawler(max_requests_per_crawl=10)

    # Page headers are attached to every request the page makes, to any origin.
    @crawler.pre_navigation_hook
    async def set_page_headers(context: PlaywrightPreNavCrawlingContext) -> None:
        await context.page.set_extra_http_headers({'X-Custom-Header': 'my-value'})

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        # `httpbin.org/headers` echoes the received request headers back.
        context.log.info(await context.response.text())

    await crawler.run(['https://httpbin.org/headers'])


if __name__ == '__main__':
    asyncio.run(main())
