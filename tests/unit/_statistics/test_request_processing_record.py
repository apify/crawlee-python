from datetime import timedelta

import pytest

from crawlee.statistics._statistics import RequestProcessingRecord


@pytest.mark.parametrize("_", range(500))
def test_tracking_time_resolution(_):
    record = RequestProcessingRecord()
    record.run()
    record.finish()
    assert record.duration > timedelta(seconds=0)
