try:
    from ._playwright_crawler import PlaywrightCrawler
    from ._playwright_crawling_context import PlaywrightCrawlingContext
    from ._playwright_pre_nav_crawling_context import PlaywrightPreNavCrawlingContext
except ImportError as exc:
    raise ImportError(
        "To import this, you need to install the 'playwright' extra. "
        "For example, if you use pip, run `pip install 'crawlee[playwright]'`.",
    ) from exc

__all__ = [
    'PlaywrightCrawler',
    'PlaywrightCrawlingContext',
    'PlaywrightPreNavCrawlingContext',
]
