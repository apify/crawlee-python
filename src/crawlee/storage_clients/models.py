from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Generic

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from typing_extensions import TypeVar

from crawlee import Request
from crawlee._types import HttpMethod
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import validate_http_url

KvsValueType = TypeVar('KvsValueType', default=Any)


@docs_group('Data structures')
class StorageMetadata(BaseModel):
    """Represents the base model for storage metadata.

    It contains common fields shared across all specific storage types.
    """

    model_config = ConfigDict(populate_by_name=True, extra='allow')

    id: Annotated[str, Field(alias='id')]
    """The unique identifier of the storage."""

    name: Annotated[str | None, Field(alias='name', default=None)]
    """The name of the storage."""

    accessed_at: Annotated[datetime, Field(alias='accessedAt')]
    """The timestamp when the storage was last accessed."""

    created_at: Annotated[datetime, Field(alias='createdAt')]
    """The timestamp when the storage was created."""

    modified_at: Annotated[datetime, Field(alias='modifiedAt')]
    """The timestamp when the storage was last modified."""


@docs_group('Data structures')
class DatasetMetadata(StorageMetadata):
    """Model for a dataset metadata."""

    model_config = ConfigDict(populate_by_name=True)

    item_count: Annotated[int, Field(alias='itemCount')]
    """The number of items in the dataset."""


@docs_group('Data structures')
class KeyValueStoreMetadata(StorageMetadata):
    """Model for a key-value store metadata."""

    model_config = ConfigDict(populate_by_name=True)


@docs_group('Data structures')
class RequestQueueMetadata(StorageMetadata):
    """Model for a request queue metadata."""

    model_config = ConfigDict(populate_by_name=True)

    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]
    """Indicates whether the queue has been accessed by multiple clients (consumers)."""

    handled_request_count: Annotated[int, Field(alias='handledRequestCount')]
    """The number of requests that have been handled from the queue."""

    pending_request_count: Annotated[int, Field(alias='pendingRequestCount')]
    """The number of requests that are still pending in the queue."""

    stats: Annotated[dict, Field(alias='stats')]
    """Statistics about the request queue, TODO?"""

    total_request_count: Annotated[int, Field(alias='totalRequestCount')]
    """The total number of requests that have been added to the queue."""


@docs_group('Data structures')
class KeyValueStoreRecordMetadata(BaseModel):
    """Model for a key-value store record metadata."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    """The key of the record.

    A unique identifier for the record in the key-value store.
    """

    content_type: Annotated[str, Field(alias='contentType')]
    """The MIME type of the record.

    Describe the format and type of data stored in the record, following the MIME specification.
    """

    size: Annotated[int, Field(alias='size')]
    """The size of the record in bytes."""


@docs_group('Data structures')
class KeyValueStoreRecord(KeyValueStoreRecordMetadata, Generic[KvsValueType]):
    """Model for a key-value store record."""

    model_config = ConfigDict(populate_by_name=True)

    value: Annotated[KvsValueType, Field(alias='value')]
    """The value of the record."""


@docs_group('Data structures')
class KeyValueStoreListKeysPage(BaseModel):
    """Model for listing keys in the key-value store."""

    model_config = ConfigDict(populate_by_name=True)

    count: Annotated[int, Field(alias='count')]
    """The number of keys returned on this page."""

    limit: Annotated[int, Field(alias='limit')]
    """The maximum number of keys to return."""

    is_truncated: Annotated[bool, Field(alias='isTruncated')]
    """Indicates whether there are more keys to retrieve."""

    exclusive_start_key: Annotated[str | None, Field(alias='exclusiveStartKey', default=None)]
    """The key from which to start this page of results."""

    next_exclusive_start_key: Annotated[str | None, Field(alias='nextExclusiveStartKey', default=None)]
    """The key from which to start the next page of results."""

    items: Annotated[list[KeyValueStoreRecordMetadata], Field(alias='items', default_factory=list)]
    """The list of KVS items metadata returned on this page."""


@docs_group('Data structures')
class RequestQueueHeadState(BaseModel):
    """Model for the request queue head state."""

    model_config = ConfigDict(populate_by_name=True)

    was_limit_reached: Annotated[bool, Field(alias='wasLimitReached')]
    prev_limit: Annotated[int, Field(alias='prevLimit')]
    queue_modified_at: Annotated[datetime, Field(alias='queueModifiedAt')]
    query_started_at: Annotated[datetime, Field(alias='queryStartedAt')]
    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]


@docs_group('Data structures')
class RequestQueueHead(BaseModel):
    """Model for request queue head.

    Represents a collection of requests retrieved from the beginning of a queue,
    including metadata about the queue's state and lock information for the requests.
    """

    model_config = ConfigDict(populate_by_name=True)

    limit: Annotated[int | None, Field(alias='limit', default=None)]
    """The maximum number of requests that were requested from the queue."""

    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients', default=False)]
    """Indicates whether the queue has been accessed by multiple clients (consumers)."""

    queue_modified_at: Annotated[datetime, Field(alias='queueModifiedAt')]
    """The timestamp when the queue was last modified."""

    lock_time: Annotated[timedelta | None, Field(alias='lockSecs', default=None)]
    """The duration for which the returned requests are locked and cannot be processed by other clients."""

    queue_has_locked_requests: Annotated[bool | None, Field(alias='queueHasLockedRequests', default=False)]
    """Indicates whether the queue contains any locked requests."""

    items: Annotated[list[Request], Field(alias='items', default_factory=list[Request])]
    """The list of request objects retrieved from the beginning of the queue."""


class _ListPage(BaseModel):
    """Model for a single page of storage items returned from a collection list method."""

    model_config = ConfigDict(populate_by_name=True)

    count: Annotated[int, Field(default=0)]
    """The number of objects returned on this page."""

    offset: Annotated[int, Field(default=0)]
    """The starting position of the first object returned, as specified in the API call."""

    limit: Annotated[int, Field(default=0)]
    """The maximum number of objects to return, as specified in the API call."""

    total: Annotated[int, Field(default=0)]
    """The total number of objects that match the criteria of the API call."""

    desc: Annotated[bool, Field(default=False)]
    """Indicates if the returned list is in descending order."""


@docs_group('Data structures')
class DatasetListPage(_ListPage):
    """Model for a single page of dataset items returned from a collection list method."""

    items: Annotated[list[DatasetMetadata], Field(default_factory=list)]
    """The list of dataset items returned on this page."""


@docs_group('Data structures')
class KeyValueStoreListPage(_ListPage):
    """Model for a single page of key-value store items returned from a collection list method."""

    items: Annotated[list[KeyValueStoreMetadata], Field(default_factory=list)]
    """The list of key-value store items returned on this page."""


@docs_group('Data structures')
class RequestQueueListPage(_ListPage):
    """Model for a single page of request queue items returned from a collection list method."""

    items: Annotated[list[RequestQueueMetadata], Field(default_factory=list)]
    """The list of request queue items returned on this page."""


@docs_group('Data structures')
class DatasetItemsListPage(_ListPage):
    """Model for a single page of dataset items returned from a collection list method."""

    items: Annotated[list[dict], Field(default_factory=list)]
    """The list of dataset items returned on this page."""


@docs_group('Data structures')
class ProlongRequestLockResponse(BaseModel):
    """Response to prolong request lock calls."""

    model_config = ConfigDict(populate_by_name=True)

    lock_expires_at: Annotated[datetime, Field(alias='lockExpiresAt')]


@docs_group('Data structures')
class ProcessedRequest(BaseModel):
    """Represents a processed request."""

    model_config = ConfigDict(populate_by_name=True)

    id: Annotated[str, Field(alias='requestId')]
    unique_key: Annotated[str, Field(alias='uniqueKey')]
    was_already_present: Annotated[bool, Field(alias='wasAlreadyPresent')]
    was_already_handled: Annotated[bool, Field(alias='wasAlreadyHandled')]


@docs_group('Data structures')
class UnprocessedRequest(BaseModel):
    """Represents an unprocessed request."""

    model_config = ConfigDict(populate_by_name=True)

    unique_key: Annotated[str, Field(alias='uniqueKey')]
    url: Annotated[str, BeforeValidator(validate_http_url), Field()]
    method: Annotated[HttpMethod | None, Field()] = None


@docs_group('Data structures')
class AddRequestsResponse(BaseModel):
    """Model for a response to add requests to a queue.

    Contains detailed information about the processing results when adding multiple requests
    to a queue. This includes which requests were successfully processed and which ones
    encountered issues during processing.
    """

    model_config = ConfigDict(populate_by_name=True)

    processed_requests: Annotated[list[ProcessedRequest], Field(alias='processedRequests')]
    """Successfully processed requests, including information about whether they were
    already present in the queue and whether they had been handled previously."""

    unprocessed_requests: Annotated[list[UnprocessedRequest], Field(alias='unprocessedRequests')]
    """Requests that could not be processed, typically due to validation errors or other issues."""


class InternalRequest(BaseModel):
    """Internal representation of a queue request with additional metadata for ordering and storage."""

    model_config = ConfigDict(populate_by_name=True)

    id: str

    unique_key: str

    order_no: Decimal | None = None
    """Order number for maintaining request sequence in queue.
    Used for restoring correct request order when recovering queue from storage."""

    handled_at: datetime | None

    request: Annotated[
        Request,
        Field(alias='json_'),
        BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
    ]
    """Original Request object. The alias 'json_' is required for backward compatibility with legacy code."""

    @classmethod
    def from_request(cls, request: Request, id: str, order_no: Decimal | None) -> InternalRequest:
        """Create an internal request from a `Request` object."""
        return cls(
            unique_key=request.unique_key,
            id=id,
            handled_at=request.handled_at,
            order_no=order_no,
            request=request,
        )

    def to_request(self) -> Request:
        """Convert the internal request back to a `Request` object."""
        return self.request


class CachedRequest(BaseModel):
    """Pydantic model for cached request information."""

    id: str
    """The ID of the request."""

    was_already_handled: bool
    """Whether the request was already handled."""

    hydrated: Request | None = None
    """The hydrated request object (the original one)."""

    lock_expires_at: datetime | None = None
    """The expiration time of the lock on the request."""

    forefront: bool = False
    """Whether the request was added to the forefront of the queue."""
