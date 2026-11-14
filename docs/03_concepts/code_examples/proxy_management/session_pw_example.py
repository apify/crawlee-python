import asyncio

from crawlee.crawlers import PlaywrightCrawler
from crawlee.proxy_configuration import ProxyConfiguration


async def main() -> None:
    # Create a ProxyConfiguration object and pass it to the crawler.
    proxy_configuration = ProxyConfiguration(
        proxy_urls=[
            'http://proxy-1.com/',
            'http://proxy-2.com/',
        ]
    )
    crawler = PlaywrightCrawler(
        proxy_configuration=proxy_configuration,
        save_response_cookies=True,
    )

    # ...


if __name__ == '__main__':
    asyncio.run(main())
