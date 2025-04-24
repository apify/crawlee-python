import asyncio
import re
import traceback


def _get_only_innermost_exception(error: BaseException) -> BaseException:
    """Get innermost exception by following __cause__ and __context__ attributes of exception."""
    if error.__cause__:
        return _get_only_innermost_exception(error.__cause__)
    if error.__context__:
        return _get_only_innermost_exception(error.__context__)
    # No __cause__ and no __context__, this is as deep as it can get.
    return error


def _get_filtered_traceback_parts_for_asyncio_timeout_error(traceback_parts: list[str]) -> list[str]:
    """Extract only the most relevant traceback parts from stack trace."""
    ignore_pattern = (
        r'([\\/]{1}asyncio[\\/]{1})|'  # internal asyncio parts
        r'(Traceback \(most recent call last\))|'  # common part of the stack trace formatting
        r'(asyncio\.exceptions\.CancelledError)'  # internal asyncio exception
    )
    return [
        _strip_pep657_highlighting(traceback_part)
        for traceback_part in traceback_parts
        if not re.findall(ignore_pattern, traceback_part)
    ]


def _strip_pep657_highlighting(traceback_part: str) -> str:
    """Remove PEP 657 highlighting from the traceback."""
    highlight_pattern = r'(\n\s*~*\^+\n)$'
    return re.sub(highlight_pattern, '\n', traceback_part)


def reduce_asyncio_timeout_error_to_relevant_traceback_parts(
    timeout_error: asyncio.exceptions.TimeoutError,
) -> list[str]:
    """Reduce the asyncio timeout traceback to most relevant parts."""
    innermost_error = _get_only_innermost_exception(timeout_error)
    traceback_parts = traceback.format_exception(
        type(innermost_error), value=innermost_error, tb=innermost_error.__traceback__, chain=True
    )
    return _get_filtered_traceback_parts_for_asyncio_timeout_error(traceback_parts)


def get_one_line_error_summary_if_possible(error: Exception) -> str:
    if isinstance(error, asyncio.exceptions.TimeoutError):
        return reduce_asyncio_timeout_error_to_relevant_traceback_parts(error)[-1].strip('\n ').replace('\n', ', ')
    return ''
