# % extends 'main.py'

# % block import
from crawlee.crawlers import HttpCrawler
# % endblock

# % block instantiation
crawler = HttpCrawler(
    request_handler=router,
    max_requests_per_crawl=10,
    {{ self.http_client_instantiation() }})
# % endblock
