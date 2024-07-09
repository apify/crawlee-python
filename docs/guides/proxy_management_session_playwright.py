from crawlee.playwright_crawler import PlaywrightCrawler
from crawlee.proxy_configuration import ProxyConfiguration

proxy_configuration = ProxyConfiguration(
    proxy_urls=['http://proxy-1.com', 'http://proxy-2.com'],
)

crawler = PlaywrightCrawler(
    persist_cookies_per_session=True,
    proxy_configuration=proxy_configuration,
)
