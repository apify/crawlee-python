try:
    from .beautifulsoup_crawler import BeautifulSoupCrawler
    from .types import BeautifulSoupCrawlingContext
except ImportError as exc:
    raise ImportError(
        'To use this module, you need to install the "beautifulsoup" extra. Run "pip install crawlee[beautifulsoup]".',
    ) from exc
