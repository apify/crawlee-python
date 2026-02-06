from __future__ import annotations

import asyncio

from crawlee.statistics import Statistics


async def test_request_max_duration_tracks_maximum() -> None:
    """Test that request_max_duration correctly tracks the maximum duration, not the minimum."""

    # asyncio.sleep() can sleep slightly shorter than expected https://bugs.python.org/issue31539#msg302699
    asyncio_sleep_time_tolerance = 0.015
    sleep_time = 0.05

    async with Statistics.with_default_state() as statistics:
        # Record a short request
        statistics.record_request_processing_start('request_1')
        statistics.record_request_processing_finish('request_1')
        first_duration = statistics.state.request_max_duration

        # Record a longer request
        statistics.record_request_processing_start('request_2')
        await asyncio.sleep(sleep_time)  # 50ms delay
        statistics.record_request_processing_finish('request_2')
        second_duration = statistics.state.request_max_duration

        # The max duration should be updated to the longer request's duration
        assert second_duration is not None
        assert first_duration is not None
        assert second_duration >= first_duration
        assert second_duration.total_seconds() >= (sleep_time - asyncio_sleep_time_tolerance)

        # Record another short request - max should NOT decrease
        statistics.record_request_processing_start('request_3')
        statistics.record_request_processing_finish('request_3')
        third_duration = statistics.state.request_max_duration

        # The max duration should remain unchanged (still the longest request)
        assert third_duration == second_duration
