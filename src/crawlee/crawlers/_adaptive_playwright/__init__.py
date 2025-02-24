from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

# These imports have only mandatory dependencies, so they are imported directly.
from ._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'BeautifulSoupCrawler'):
    from ._rendering_type_predictor import RenderingType, RenderingTypePrediction, RenderingTypePredictor
with _try_import(__name__, 'BeautifulSoupCrawlingContext'):
    from ._adaptive_playwright_crawler import AdaptivePlaywrightCrawler

__all__ = [
    'AdaptivePlaywrightCrawler',
    'AdaptivePlaywrightCrawlingContext',
    'AdaptivePlaywrightPreNavCrawlingContext',
    'RenderingType',
    'RenderingTypePrediction',
    'RenderingTypePredictor',
]
