# % extends 'main.py'

# % block import
from crawlee.crawlers import ParselCrawler
# % endblock

# % block instantiation
crawler = ParselCrawler(
    request_handler=router,
    max_requests_per_crawl=50,
    {{ self.http_client_instantiation() }})
# % endblock
