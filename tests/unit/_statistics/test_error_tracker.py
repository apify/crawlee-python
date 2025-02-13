from crawlee.statistics._error_tracker import ErrorTracker


def test_error_tracker():
    error_tracker = ErrorTracker()
    error_1 = ValueError("Some value error")
    error_2 = ValueError("Some value error")
    error_3 = ValueError("Another value error")

    error_tracker.add(error_1)
    error_tracker.add(error_2)
    error_tracker.add(error_3)

    assert error_tracker.total == 3
    assert error_tracker.unique_error_count == 1
