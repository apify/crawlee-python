# % extends 'main.py'

# % block import
from crawlee.crawlers import AdaptivePlaywrightCrawler
# % endblock

# % block instantiation
crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
    request_handler=router,
    max_requests_per_crawl=10,
    {{ self.http_client_instantiation() }})
# % endblock
