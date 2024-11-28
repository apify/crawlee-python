from datetime import datetime, timedelta

from dateutil import tz

from crawlee.statistics import StatisticsPersistedState, StatisticsState


def test_statistics_state_timedelta_ms_explicit_none() -> None:
    statistics_state = StatisticsState(
        request_min_duration=None,
        request_max_duration=None,
        crawler_finished_at=None,
    )

    statistics_persisted_state = StatisticsPersistedState(
        request_retry_histogram=[],
        stats_id=1,
        request_avg_failed_duration=None,
        request_avg_finished_duration=None,
        request_total_duration=0,
        requests_total=1,
        crawler_last_started_at=datetime.now(tz=tz.UTC),
        stats_persisted_at=datetime.now(tz=tz.UTC),
    )

    assert statistics_persisted_state.request_avg_failed_duration is None
    assert statistics_persisted_state.request_avg_finished_duration is None
    assert statistics_persisted_state.request_total_duration == timedelta(0)
    statistics_persisted_state.model_dump()

    assert statistics_state.request_max_duration is None
    assert statistics_state.request_min_duration is None
    assert statistics_state.crawler_finished_at is None
    assert statistics_state.crawler_runtime == timedelta(0)
    statistics_state.model_dump()


def test_statistics_state_timedelta_ms_implicit_none() -> None:
    statistics_state = StatisticsState()

    assert statistics_state.request_max_duration is None
    assert statistics_state.request_min_duration is None
    assert statistics_state.crawler_finished_at is None
    assert statistics_state.crawler_runtime == timedelta(0)
    statistics_state.model_dump()
