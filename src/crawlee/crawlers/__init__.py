import contextlib as _contextlib
import sys as _sys
from types import ModuleType as _ModuleType
from typing import Any as _Any

from ._abstract_http import AbstractHttpCrawler, AbstractHttpParser, HttpCrawlerOptions, ParsedHttpCrawlingContext
from ._basic import BasicCrawler, BasicCrawlerOptions, BasicCrawlingContext, BlockedInfo, ContextPipeline
from ._http import HttpCrawler, HttpCrawlingContext, HttpCrawlingResult

with _contextlib.suppress(ImportError):
    from ._beautifulsoup import BeautifulSoupCrawler, BeautifulSoupCrawlingContext, BeautifulSoupParserType

with _contextlib.suppress(ImportError):
    from ._parsel import ParselCrawler, ParselCrawlingContext

with _contextlib.suppress(ImportError):
    from ._playwright import PlaywrightCrawler, PlaywrightCrawlingContext, PlaywrightPreNavCrawlingContext


class _ModuleWrapper(_ModuleType):
    def __getattribute__(self, attr: str) -> _Any:
        result = super().__getattribute__(attr)

        if isinstance(result, Exception):
            raise result

        return result


_sys.modules[__name__].__class__ = _ModuleWrapper
