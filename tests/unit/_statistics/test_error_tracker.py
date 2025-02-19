import traceback

import pytest

from crawlee.statistics._error_tracker import ErrorTracker


@pytest.mark.parametrize(('error_tracker', 'expected_unique_errors'), [
    (ErrorTracker(), 4),
    (ErrorTracker(show_stack_trace=False), 3),
    (ErrorTracker(show_error_name=False), 3),
    (ErrorTracker(show_error_message=False), 3),
    (ErrorTracker(show_stack_trace=False, show_error_name=False), 2),
    (ErrorTracker(show_stack_trace=False, show_error_message=False), 2),
    (ErrorTracker(show_stack_trace=False, show_error_name=False, show_error_message=False), 1),

])
def test_error_tracker_counts(error_tracker: ErrorTracker, expected_unique_errors: int) -> None:
    """Use different settings of `error_tracker` and test unique errors count."""

    for error in [
        Exception('Some value error abc'),
        ValueError('Some value error abc'),  # Different type, different error
        ValueError('Some value error cde'),  # Same type and similar message to previous, considered the same.
        ValueError('Another value error efg')  # Same type, but too different message to previous, considered different.
    ]:
        try:
            raise error  # Errors raised on same line
        except Exception as e:  # noqa:PERF203
            error_tracker.add(e)

    try:
        raise ValueError('Some value error abc')  # Same as one previous error, but different line.
    except Exception as e:
        error_tracker.add(e)

    assert error_tracker.total == 5
    assert error_tracker.unique_error_count == expected_unique_errors

@pytest.mark.parametrize(('message_1', 'message_2', 'expected_generic_message'), [
    ('Some error number 123', 'Some error number 456', 'Some error number _'),
    ('Some error number 123 456', 'Some error number 123 456 789', 'Some error number 123 456 _'),
    ('Some error number 0 0 0', 'Some error number 1 0 1', 'Some error number _ 0 _'),
])
def test_error_tracker_similar_messages(message_1: str, message_2: str, expected_generic_message: str) -> None:
    """Test that similar messages collapse into same group with generic name that contains wildcards."""
    line: int
    error_tracker = ErrorTracker()
    for error in [
        KeyError(message_1),
        KeyError(message_1),
        KeyError(message_1),
        ValueError(message_1),
        ValueError(message_2),
        RuntimeError(message_2),
    ]:
        try:
            raise error  # Errors raised on the same line
        except Exception as e:  # noqa:PERF203
            error_tracker.add(e)
            line = traceback.extract_tb(e.__traceback__)[0].lineno

    errors = error_tracker.get_most_popular_errors()
    assert errors[0][0] == f'{__file__}:{line}:KeyError:{message_1}'
    assert errors[0][1] == 3
    assert errors[1][0] == f'{__file__}:{line}:ValueError:{expected_generic_message}'
    assert errors[1][1] == 2
    assert errors[2][0] == f'{__file__}:{line}:RuntimeError:{message_2}'
    assert errors[2][1] == 1
