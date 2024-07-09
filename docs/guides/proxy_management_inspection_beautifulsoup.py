from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration

proxy_configuration = ProxyConfiguration(
    proxy_urls=['http://proxy-1.com', 'http://proxy-2.com'],
)

crawler = BeautifulSoupCrawler(
    proxy_configuration=proxy_configuration,
)

@crawler.router.default_handler
async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
    print(context.proxy_info)
