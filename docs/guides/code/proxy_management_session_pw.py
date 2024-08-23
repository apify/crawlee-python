from crawlee.playwright_crawler import PlaywrightCrawler
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
        use_session_pool=True,
    )
