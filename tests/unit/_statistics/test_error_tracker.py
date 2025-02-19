import pytest


from crawlee.statistics._error_tracker import ErrorTracker


@pytest.mark.parametrize("error_tracker, expected_unique_errors", [
    (ErrorTracker(), 4),
    (ErrorTracker(show_stack_trace=False), 3),
    (ErrorTracker(show_error_name=False), 3),
    (ErrorTracker(show_error_message=False), 3),
    (ErrorTracker(show_stack_trace=False, show_error_name=False), 2),
    (ErrorTracker(show_stack_trace=False, show_error_message=False), 2),
    (ErrorTracker(show_stack_trace=False, show_error_name=False, show_error_message=False), 1),

])
def test_error_tracker_counts(error_tracker: ErrorTracker, expected_unique_errors: int):
    """Use different settings of `error_tracker` and test unique errors count."""

    for error in [
        Exception("Some value error abc"),
        ValueError("Some value error abc"),  # Different type, different error
        ValueError("Some value error cde"),  # Same type and similar message to previous, considered the same.
        ValueError("Another value error efg")  # Same type, but too different message to previous, considered different.
    ]:
        try:
            raise error  # Errors raised on same line
        except Exception as e:
            error_tracker.add(e)

    try:
        raise ValueError("Some value error abc")  # Same as one previous error, but different line.
    except Exception as e:
        error_tracker.add(e)

    assert error_tracker.total == 5
    assert error_tracker.unique_error_count == expected_unique_errors

@pytest.mark.parametrize("message_1, message_2, expected_generic_message", [
    ("Some error number 123", "Some error number 456", "Some error number _"),
])
def test_error_tracker_similar_messages(message_1, message_2, expected_generic_message):
    """Test that similar messages collapse into same group with generic name that contains wildcards."""
    error_tracker = ErrorTracker()
    for error in [
        ValueError(message_1),
        ValueError(message_2),
    ]:
        try:
            raise error  # Errors raised on same line
        except Exception as e:
            error_tracker.add(e)

    assert error_tracker.total == 2
    assert error_tracker.unique_error_count == 1
    # TODO test after most popular errors
