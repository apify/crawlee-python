from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatisticState,
)
from crawlee.statistics import Statistics


async def test_predictor_state_persistence() -> None:
    """Test that adaptive statistics can be correctly persisted and initialized from persisted values."""

    async with Statistics(
        state_model=AdaptivePlaywrightCrawlerStatisticState, persistence_enabled=True
    ) as adaptive_statistics:
        adaptive_statistics.state.browser_request_handler_runs = 1
        adaptive_statistics.state.rendering_type_mispredictions = 2
        adaptive_statistics.state.http_only_request_handler_runs = 3

        persistence_state_key = adaptive_statistics._persist_state_key
    # adaptive_statistics are persisted after leaving the context

    # new_adaptive_statistics are initialized from the persisted values.
    async with Statistics(
        state_model=AdaptivePlaywrightCrawlerStatisticState,
        persistence_enabled=True,
        persist_state_key=persistence_state_key,
    ) as new_adaptive_statistics:
        pass

    assert new_adaptive_statistics.state.browser_request_handler_runs == 1
    assert new_adaptive_statistics.state.rendering_type_mispredictions == 2
    assert new_adaptive_statistics.state.http_only_request_handler_runs == 3
