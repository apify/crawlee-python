# % extends 'main.py'

# % block import
from crawlee.parsel_crawler import ParselCrawler
# % endblock

# % block instantiation
crawler = ParselCrawler(
    request_handler=router,
    max_requests_per_crawl=50,
)
# % endblock
