from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

from ._abstract_http import AbstractHttpCrawler, AbstractHttpParser, ParsedHttpCrawlingContext
from ._basic import BasicCrawler, BasicCrawlerOptions, BasicCrawlingContext, ContextPipeline
from ._http import HttpCrawler, HttpCrawlingContext, HttpCrawlingResult

_install_import_hook(__name__)

# The following imports use try_import to handle optional dependencies, as they may not always be available.

with _try_import(__name__, 'BeautifulSoupCrawler', 'BeautifulSoupCrawlingContext', 'BeautifulSoupParserType'):
    from ._beautifulsoup import BeautifulSoupCrawler, BeautifulSoupCrawlingContext, BeautifulSoupParserType

with _try_import(__name__, 'ParselCrawler', 'ParselCrawlingContext'):
    from ._parsel import ParselCrawler, ParselCrawlingContext

with _try_import(__name__, 'PlaywrightCrawler', 'PlaywrightCrawlingContext', 'PlaywrightPreNavCrawlingContext'):
    from ._playwright import PlaywrightCrawler, PlaywrightCrawlingContext, PlaywrightPreNavCrawlingContext

with _try_import(
    __name__,
    'AdaptivePlaywrightCrawler',
    'AdaptivePlaywrightCrawlingContext',
    'AdaptivePlaywrightPreNavCrawlingContext',
    'RenderingType',
    'RenderingTypePrediction',
    'RenderingTypePredictor',
):
    from ._adaptive_playwright import (
        AdaptivePlaywrightCrawler,
        AdaptivePlaywrightCrawlingContext,
        AdaptivePlaywrightPreNavCrawlingContext,
        RenderingType,
        RenderingTypePrediction,
        RenderingTypePredictor,
    )


__all__ = [
    'AbstractHttpCrawler',
    'AbstractHttpParser',
    'AdaptivePlaywrightCrawler',
    'AdaptivePlaywrightCrawlingContext',
    'AdaptivePlaywrightPreNavCrawlingContext',
    'BasicCrawler',
    'BasicCrawlerOptions',
    'BasicCrawlingContext',
    'BeautifulSoupCrawler',
    'BeautifulSoupCrawlingContext',
    'BeautifulSoupParserType',
    'ContextPipeline',
    'HttpCrawler',
    'HttpCrawlingContext',
    'HttpCrawlingResult',
    'ParsedHttpCrawlingContext',
    'ParselCrawler',
    'ParselCrawlingContext',
    'PlaywrightCrawler',
    'PlaywrightCrawlingContext',
    'PlaywrightPreNavCrawlingContext',
    'RenderingType',
    'RenderingTypePrediction',
    'RenderingTypePredictor',
]
