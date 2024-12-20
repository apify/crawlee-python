import contextlib as _contextlib

from ._abstract_http import AbstractHttpCrawler, AbstractHttpParser, HttpCrawlerOptions, ParsedHttpCrawlingContext
from ._basic import BasicCrawler, BasicCrawlerOptions, BasicCrawlingContext, BlockedInfo, ContextPipeline
from ._http import HttpCrawler, HttpCrawlingContext, HttpCrawlingResult

with _contextlib.suppress(ImportError):
    from ._beautifulsoup import BeautifulSoupCrawler, BeautifulSoupCrawlingContext, BeautifulSoupParserType

with _contextlib.suppress(ImportError):
    from ._parsel import ParselCrawler, ParselCrawlingContext

with _contextlib.suppress(ImportError):
    from ._playwright import PlaywrightCrawler, PlaywrightCrawlingContext, PlaywrightPreNavCrawlingContext
