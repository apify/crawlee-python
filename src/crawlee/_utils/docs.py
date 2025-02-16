from __future__ import annotations

from typing import Callable, Literal

GroupName = Literal['Classes', 'Abstract classes', 'Data structures', 'Event payloads', 'Errors', 'Functions']


def docs_group(group_name: GroupName) -> Callable:  # noqa: ARG001
    """Decorator to mark symbols for rendering and grouping in documentation.

    This decorator is used purely for documentation purposes and does not alter the behavior
    of the decorated callable.
    """

    def wrapper(func: Callable) -> Callable:
        return func

    return wrapper
