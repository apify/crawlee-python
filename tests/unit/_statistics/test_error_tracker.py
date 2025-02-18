import traceback

from crawlee.statistics._error_tracker import ErrorTracker


def test_error_tracker():
    error_tracker = ErrorTracker()
    for error in [ValueError("Some value error"), ValueError("Some value error"), ValueError("Another value error")]:
        try:
            raise error
        except Exception:
            error_tracker.add(error)

    assert error_tracker.total == 3
    assert error_tracker.unique_error_count == 1
