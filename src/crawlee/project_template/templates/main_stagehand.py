# % extends 'main.py'

# % block import
from crawlee.browsers import StagehandOptions
from crawlee.crawlers import StagehandCrawler
# % endblock

# % block instantiation
crawler = StagehandCrawler(
    request_handler=router,
    headless=True,
    max_requests_per_crawl=10,
    stagehand_options=StagehandOptions(
        model_api_key='<YOUR_OPENAI_API_KEY>',
    ),
    {{ self.http_client_instantiation() }})
# % endblock
