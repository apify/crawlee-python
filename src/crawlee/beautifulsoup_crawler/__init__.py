try:
    from .beautifulsoup_crawler import BeautifulSoupCrawler
    from .types import BeautifulSoupCrawlingContext
except ImportError as exc:
    raise ImportError(
        'To import anything from this subpacakge, you need to install the "beautifulsoup" extra. '
        'For example, if you use pip, run "pip install crawlee[beautifulsoup]".',
    ) from exc
