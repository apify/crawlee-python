from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'PlaywrightCrawler'):
    from ._playwright_crawler import PlaywrightCrawler
with _try_import(__name__, 'PlaywrightCrawlingContext'):
    from ._playwright_crawling_context import PlaywrightCrawlingContext
with _try_import(__name__, 'PlaywrightPreNavCrawlingContext'):
    from ._playwright_pre_nav_crawling_context import PlaywrightPreNavCrawlingContext

__all__ = [
    'PlaywrightCrawler',
    'PlaywrightCrawlingContext',
    'PlaywrightPreNavCrawlingContext',
]
