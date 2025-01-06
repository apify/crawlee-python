from __future__ import annotations

from itertools import cycle
from typing import TYPE_CHECKING

import pytest
from typing_extensions import override

from crawlee.crawlers._adaptive_playwright import AdaptivePlaywrightCrawler, AdaptivePlaywrightCrawlingContext
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import AdaptiveContextError
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import (
    RenderingType,
    RenderingTypePrediction,
    RenderingTypePredictor,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from crawlee._types import BasicCrawlingContext
    from crawlee.crawlers import PlaywrightPreNavCrawlingContext


class _SimpleRenderingTypePredictor(RenderingTypePredictor):
    """Simplified predictor for tests."""

    def __init__(self, rendering_types: Iterator[RenderingType],
                 detection_probability_recommendation: Iterator[int]) -> None:
        self._rendering_types = rendering_types
        self._detection_probability_recommendation = detection_probability_recommendation

    @override
    def predict(self, url: str, label: str | None) -> RenderingTypePrediction:
        return RenderingTypePrediction(next(self._rendering_types), next(self._detection_probability_recommendation))

    @override
    def store_result(self, url: str, label: str | None, crawl_type: RenderingType) -> None:
        pass



@pytest.mark.parametrize(('expected_pw_count', 'expected_bs_count', 'rendering_types'), [
    (0,2, cycle(['static'])),
    (2,0, cycle(['client only'])),
    (1,1, cycle(['static','client only'])),
])
async def test_adaptive_crawling(expected_pw_count: int, expected_bs_count: int,
                                 rendering_types: Iterator[RenderingType]) -> None:
    """Tests correct routing to pre-nav hooks and correct handling through proper handler."""
    static_only_predictor = _SimpleRenderingTypePredictor(
        rendering_types = rendering_types,
        detection_probability_recommendation=cycle([0])
    )


    crawler = AdaptivePlaywrightCrawler(rendering_type_predictor=static_only_predictor)

    pw_handler_count = 0
    bs_handler_count = 0

    pw_hook_count = 0
    bs_hook_count = 0

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal pw_handler_count
        nonlocal bs_handler_count

        try:
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            pw_handler_count += 1
        except AdaptiveContextError:
            bs_handler_count += 1


    @crawler.pre_navigation_hook_bs
    async def bs_hook(context: BasicCrawlingContext) -> None:  # noqa:ARG001  # Intentionally unused arg
        nonlocal bs_hook_count
        bs_hook_count += 1

    @crawler.pre_navigation_hook_pw
    async def pw_hook(context: PlaywrightPreNavCrawlingContext) -> None:  # noqa:ARG001  # Intentionally unused arg
        nonlocal pw_hook_count
        pw_hook_count += 1


    await crawler.run(['https://crawlee.dev/',
                       'https://crawlee.dev/docs/quick-start'])

    assert pw_handler_count == expected_pw_count
    assert pw_hook_count == expected_pw_count

    assert bs_handler_count == expected_bs_count
    assert bs_hook_count == expected_bs_count
