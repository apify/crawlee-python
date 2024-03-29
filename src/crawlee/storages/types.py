from __future__ import annotations

from enum import Enum
from typing import Any, Generic, TypeVar, Union

T = TypeVar('T')


# Type for representing json-serializable values. It's close enough to the real thing supported
# by json.parse, and the best we can do until mypy supports recursive types. It was suggested
# in a discussion with (and approved by) Guido van Rossum, so I'd consider it correct enough.
JSONSerializable = Union[str, int, float, bool, None, dict[str, Any], list[Any]]


class StorageTypes(str, Enum):
    """Possible Crawlee storage types."""

    DATASET = 'Dataset'
    KEY_VALUE_STORE = 'Key-value store'
    REQUEST_QUEUE = 'Request queue'


class ListPage(Generic[T]):
    """A single page of items returned from a list() method.

    Attributes:
        items: List of returned objects on this page
        count: Count of the returned objects on this page
        offset: The limit on the number of returned objects offset specified in the API call
        limit: The offset of the first object specified in the API call
        total: Total number of objects matching the API call criteria
        desc: Whether the listing is descending or not
    """

    items: list[T]
    count: int
    offset: int
    limit: int
    total: int
    desc: bool

    def __init__(self, data: dict) -> None:
        """Initialize a new instance from the API response data."""
        self.items = data.get('items', [])
        self.offset = data.get('offset', 0)
        self.limit = data.get('limit', 0)
        self.count = data.get('count', len(self.items))
        self.total = data.get('total', self.offset + self.count)
        self.desc = data.get('desc', False)
