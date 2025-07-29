from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, TypeVar

# The order of the rendered API groups is defined in the website/docusaurus.config.js file.
GroupName = Literal[
    'Autoscaling',
    'Browser management',
    'Configuration',
    'Crawlers',
    'Crawling contexts',
    'Errors',
    'Event data',
    'Event managers',
    'Functions',
    'HTTP clients',
    'HTTP parsers',
    'Request loaders',
    'Session management',
    'Statistics',
    'Storage clients',
    'Storage data',
    'Storages',
    'Other',
]

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
