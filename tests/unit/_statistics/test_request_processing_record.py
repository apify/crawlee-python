from datetime import timedelta

from crawlee.statistics._statistics import RequestProcessingRecord


def test_tracking_time_resolution() -> None:
    """Test that `RequestProcessingRecord` tracks time with sufficient resolution.

    This is generally not an issue on Linux, but on Windows some packages in older Python versions might be using system
    timers with not so granular resolution - some sources estimate 15ms. This test will start failing on Windows
    if unsuitable source of time measurement is selected due to two successive time measurements possibly using same
    timing sample."""
    record = RequestProcessingRecord()
    record.run()
    record.finish()
    assert record.duration
    assert record.duration > timedelta(seconds=0)
