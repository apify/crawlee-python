from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, TypeVar

GroupName = Literal['Classes', 'Abstract classes', 'Data structures', 'Event payloads', 'Errors', 'Functions']

T = TypeVar('T', bound=Callable[..., Any])


def docs_group(group_name: GroupName) -> Callable[[T], T]:  # noqa: ARG001
    """Mark a symbol for rendering and grouping in documentation.

    This decorator is used solely for documentation purposes and does not modify the behavior
    of the decorated callable.

    Args:
        group_name: The documentation group to which the symbol belongs.

    Returns:
        The original callable without modification.
    """

    def wrapper(func: T) -> T:
        return func

    return wrapper
