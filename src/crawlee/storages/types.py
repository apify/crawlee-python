from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar, Union

if TYPE_CHECKING:
    from datetime import datetime

    from crawlee.request import Request

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


@dataclass
class KeyValueStoreRecord:
    """Type definition for a key-value store record."""

    key: str
    value: Any
    content_type: str | None = None
    filename: str | None = None


@dataclass
class KeyValueStoreRecordInfo:
    """Information about a key-value store record."""

    key: str
    size: int


@dataclass
class KeyValueStoreListKeysOutput(Generic[T]):
    """Represents the output of listing keys in the key-value store."""

    count: int
    limit: int
    is_truncated: bool
    items: list[T] = field(default_factory=list)
    exclusive_start_key: str | None = None
    next_exclusive_start_key: str | None = None


@dataclass
class RequestQueueOperationInfo:
    """Result of adding a request to the queue."""

    request_id: str
    request_unique_key: str
    was_already_present: bool
    was_already_handled: bool


@dataclass
class RequestQueueSnapshot:
    """Information about the head of the request queue."""

    was_limit_reached: bool
    prev_limit: int
    queue_modified_at: datetime
    query_started_at: datetime
    had_multiple_clients: bool


@dataclass
class RequestQueueHeadResponse:
    """Response for getting the head of the request queue."""

    limit: int | None
    had_multiple_clients: bool
    queue_modified_at: datetime
    items: list[Request] = field(default_factory=list)


@dataclass
class BaseResourceInfo:
    """Base class for resource information."""

    id: str
    name: str
    accessed_at: datetime
    created_at: datetime
    modified_at: datetime


@dataclass
class DatasetResourceInfo(BaseResourceInfo):
    """Dataset resource information."""

    item_count: int


@dataclass
class KeyValueStoreResourceInfo(BaseResourceInfo):
    """Key-value store resource information."""

    user_id: str


@dataclass
class RequestQueueResourceInfo(BaseResourceInfo):
    """Resource information."""

    had_multiple_clients: bool
    handled_request_count: int
    pending_request_count: int
    stats: dict[str, Any]
    total_request_count: int
    user_id: str
    resource_directory: str


@dataclass
class ListPage(Generic[T]):
    """A single page of items returned from a list() method.

    Args:
        items: List of returned objects on this page.
        count: Count of the returned objects on this page.
        offset: The limit on the number of returned objects offset specified in the API call.
        limit: The offset of the first object specified in the API call.
        total: Total number of objects matching the API call criteria.
        desc: Whether the listing is descending or not.
    """

    items: list[T]
    count: int
    offset: int
    limit: int
    total: int
    desc: bool

    @classmethod
    def from_dict(cls, data: dict) -> ListPage:
        """Initialize a new instance from the API response data."""
        items = data.get('items', [])
        offset = data.get('offset', 0)
        limit = data.get('limit', 0)
        count = data.get('count', len(items))
        total = data.get('total', offset + count)
        desc = data.get('desc', False)
        return cls(items=items, count=count, offset=offset, limit=limit, total=total, desc=desc)
