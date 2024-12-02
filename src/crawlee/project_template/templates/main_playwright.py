# % extends 'main.py'

# % block import
from crawlee.playwright_crawler import PlaywrightCrawler
# % endblock

# % block instantiation
crawler = PlaywrightCrawler(
    request_handler=router,
    headless=True,
    max_requests_per_crawl=50,
    {{ self.http_client_instantiation() }})
# % endblock
