try:
    from ._beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupParser
except ImportError as exc:
    raise ImportError(
        "To import anything from this subpackage, you need to install the 'beautifulsoup' extra."
        "For example, if you use pip, run `pip install 'crawlee[beautifulsoup]'`.",
    ) from exc

__all__ = ['BeautifulSoupCrawler', 'BeautifulSoupParser']
