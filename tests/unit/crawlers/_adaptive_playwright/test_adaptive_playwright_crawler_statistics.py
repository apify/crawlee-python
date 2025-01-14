from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatistics,
)


async def test_predictor_state_persistence() -> None:
    """Test that adaptive statistics can be correctly persisted and initialized from persisted values."""

    async with AdaptivePlaywrightCrawlerStatistics(persistence_enabled=True) as adaptive_statistics:
        adaptive_statistics.predictor_state.track_rendering_type_mispredictions()
        adaptive_statistics.predictor_state.track_rendering_type_mispredictions()

        adaptive_statistics.predictor_state.track_http_only_request_handler_runs()
        adaptive_statistics.predictor_state.track_http_only_request_handler_runs()
        adaptive_statistics.predictor_state.track_http_only_request_handler_runs()

        adaptive_statistics.predictor_state.track_browser_request_handler_runs()

        persistence_state_key = adaptive_statistics._persist_state_key
    # adaptive_statistics are persisted after leaving the context

    # new_adaptive_statistics are initialized from the persisted values.
    async with AdaptivePlaywrightCrawlerStatistics(
        persistence_enabled=True, persist_state_key=persistence_state_key
    ) as new_adaptive_statistics:
        pass

    assert new_adaptive_statistics.predictor_state.rendering_type_mispredictions == 2
    assert new_adaptive_statistics.predictor_state.http_only_request_handler_runs == 3
    assert new_adaptive_statistics.predictor_state.browser_request_handler_runs == 1