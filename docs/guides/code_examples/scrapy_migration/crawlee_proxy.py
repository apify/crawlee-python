import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration


async def main() -> None:
    # `ProxyConfiguration` replaces Scrapy's `HttpProxyMiddleware` and the
    # `scrapy-rotating-proxies` package. The URLs rotate in a round-robin fashion.
    proxy_configuration = ProxyConfiguration(
        proxy_urls=[
            'http://proxy-1.com/',
            'http://proxy-2.com/',
        ]
    )

    crawler = ParselCrawler(
        proxy_configuration=proxy_configuration,
        max_requests_per_crawl=50,
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')
        await context.enqueue_links(selector='li.next a')

    await crawler.run(['https://quotes.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
