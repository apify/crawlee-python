# % extends 'main.py'

# % block import
import os

from crawlee.browsers import StagehandOptions
from crawlee.crawlers import StagehandCrawler
# % endblock

# % block instantiation
model_api_key = os.environ.get('OPENAI_API_KEY')
if model_api_key is None:
    raise ValueError('The OPENAI_API_KEY environment variable is not set.')

crawler = StagehandCrawler(
    request_handler=router,
    headless=True,
    max_requests_per_crawl=10,
    stagehand_options=StagehandOptions(
        model_api_key=model_api_key,
    ),
    {{ self.http_client_instantiation() }})
# % endblock
