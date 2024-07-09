from crawlee.http_crawler import HttpCrawler, HttpCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration

proxy_configuration = ProxyConfiguration(
    proxy_urls=['http://proxy-1.com', 'http://proxy-2.com'],
)

crawler = HttpCrawler(
    proxy_configuration=proxy_configuration,
)

@crawler.router.default_handler
async def request_handler(context: HttpCrawlingContext) -> None:
    print(context.proxy_info)
