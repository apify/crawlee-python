try:
    from ._rendering_type_predictor import RenderingType, RenderingTypePrediction, RenderingTypePredictor
except ImportError as exc:
    raise ImportError(
        "To import this, you need to install the 'adaptive-playwright' extra. "
        "For example, if you use pip, run `pip install 'crawlee[adaptive-playwright]'`.",
    ) from exc

from ._adaptive_playwright_crawler import AdaptivePlaywrightCrawler
from ._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)

__all__ = [
    'AdaptivePlaywrightCrawler',
    'AdaptivePlaywrightCrawlingContext',
    'AdaptivePlaywrightPreNavCrawlingContext',
    'RenderingType',
    'RenderingTypePrediction',
    'RenderingTypePredictor',
]
