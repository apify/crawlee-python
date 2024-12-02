# % extends 'main.py'

# % block import
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler
# % endblock

# % block instantiation
crawler = BeautifulSoupCrawler(
    request_handler=router,
    max_requests_per_crawl=50,
    {{ self.http_client_instantiation() }})
# % endblock
