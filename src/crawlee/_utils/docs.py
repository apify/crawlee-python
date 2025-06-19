from __future__ import annotations

from typing import Callable, Literal

GroupName = Literal['Classes', 'Abstract classes', 'Data structures', 'Event payloads', 'Errors', 'Functions']


def docs_group(group_name: GroupName) -> Callable:  # noqa: ARG001
    """Mark a symbol for rendering and grouping in documentation.

    This decorator is used solely for documentation purposes and does not modify the behavior
    of the decorated callable.

    Args:
        group_name: The documentation group to which the symbol belongs.

    Returns:
        The original callable without modification.
    """

    def wrapper(func: Callable) -> Callable:
        return func

    return wrapper
