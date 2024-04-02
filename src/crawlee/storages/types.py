from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar, Union

if TYPE_CHECKING:
    from datetime import datetime

    from crawlee.models import RequestData

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
    items: list[RequestData] = field(default_factory=list)


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
