from __future__ import annotations

from typing import Any, Dict, Generic, List, TypeVar, Union

T = TypeVar('T')


# Type for representing json-serializable values.
# It's close enough to the real thing supported by json.parse,
# and the best we can do until mypy supports recursive types.
# It was suggested in a discussion with (and approved by) Guido van Rossum,
# so I'd consider it correct enough.
JSONSerializable = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class ListPage(Generic[T]):
    """A single page of items returned from a list() method."""

    #: list: List of returned objects on this page
    items: list[T]
    #: int: Count of the returned objects on this page
    count: int
    #: int: The limit on the number of returned objects offset specified in the API call
    offset: int
    #: int: The offset of the first object specified in the API call
    limit: int
    #: int: Total number of objects matching the API call criteria
    total: int
    #: bool: Whether the listing is descending or not
    desc: bool

    def __init__(self, data: dict) -> None:
        """Initialize a ListPage instance from the API response data."""
        self.items = data.get('items', [])
        self.offset = data.get('offset', 0)
        self.limit = data.get('limit', 0)
        self.count = data.get('count', len(self.items))
        self.total = data.get('total', self.offset + self.count)
        self.desc = data.get('desc', False)
