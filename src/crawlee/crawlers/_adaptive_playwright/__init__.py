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
with _try_import(__name__, 'RenderingType', 'RenderingTypePrediction', 'RenderingTypePredictor'):
    from ._rendering_type_predictor import RenderingType, RenderingTypePrediction, RenderingTypePredictor
with _try_import(__name__, 'AdaptivePlaywrightCrawler'):
    from ._adaptive_playwright_crawler import AdaptivePlaywrightCrawler
with _try_import(__name__, 'AdaptivePlaywrightCrawlerStatisticState'):
    from ._adaptive_playwright_crawler import AdaptivePlaywrightCrawlerStatisticState

__all__ = [
    'AdaptivePlaywrightCrawler',
    'AdaptivePlaywrightCrawlerStatisticState',
    'AdaptivePlaywrightCrawlingContext',
    'AdaptivePlaywrightPreNavCrawlingContext',
    'RenderingType',
    'RenderingTypePrediction',
    'RenderingTypePredictor',
]
