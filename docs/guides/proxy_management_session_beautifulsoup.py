from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler
from crawlee.proxy_configuration import ProxyConfiguration

proxy_configuration = ProxyConfiguration(
    proxy_urls=['http://proxy-1.com', 'http://proxy-2.com'],
)

crawler = BeautifulSoupCrawler(
    persist_cookies_per_session=True,
    proxy_configuration=proxy_configuration,
)
