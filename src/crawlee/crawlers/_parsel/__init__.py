from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'ParselCrawler'):
    from ._parsel_crawler import ParselCrawler
with _try_import(__name__, 'ParselCrawlingContext'):
    from ._parsel_crawling_context import ParselCrawlingContext

__all__ = [
    'ParselCrawler',
    'ParselCrawlingContext',
]
