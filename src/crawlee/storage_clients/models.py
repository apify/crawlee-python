from __future__ import annotations

import json
from datetime import datetime
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
    name: Annotated[str | None, Field(alias='name', default='')]
    accessed_at: Annotated[datetime, Field(alias='accessedAt')]
    created_at: Annotated[datetime, Field(alias='createdAt')]
    modified_at: Annotated[datetime, Field(alias='modifiedAt')]


@docs_group('Data structures')
class DatasetMetadata(StorageMetadata):
    """Model for a dataset metadata."""

    model_config = ConfigDict(populate_by_name=True)

    item_count: Annotated[int, Field(alias='itemCount')]


@docs_group('Data structures')
class KeyValueStoreMetadata(StorageMetadata):
    """Model for a key-value store metadata."""

    model_config = ConfigDict(populate_by_name=True)

    user_id: Annotated[str, Field(alias='userId')]


@docs_group('Data structures')
class RequestQueueMetadata(StorageMetadata):
    """Model for a request queue metadata."""

    model_config = ConfigDict(populate_by_name=True)

    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]
    handled_request_count: Annotated[int, Field(alias='handledRequestCount')]
    pending_request_count: Annotated[int, Field(alias='pendingRequestCount')]
    stats: Annotated[dict, Field(alias='stats')]
    total_request_count: Annotated[int, Field(alias='totalRequestCount')]
    user_id: Annotated[str, Field(alias='userId')]
    resource_directory: Annotated[str, Field(alias='resourceDirectory')]


@docs_group('Data structures')
class KeyValueStoreRecord(BaseModel, Generic[KvsValueType]):
    """Model for a key-value store record."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    value: Annotated[KvsValueType, Field(alias='value')]
    content_type: Annotated[str | None, Field(alias='contentType', default=None)]
    filename: Annotated[str | None, Field(alias='filename', default=None)]


@docs_group('Data structures')
class KeyValueStoreRecordMetadata(BaseModel):
    """Model for a key-value store record metadata."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    content_type: Annotated[str, Field(alias='contentType')]


@docs_group('Data structures')
class KeyValueStoreKeyInfo(BaseModel):
    """Model for a key-value store key info."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    size: Annotated[int, Field(alias='size')]


@docs_group('Data structures')
class KeyValueStoreListKeysPage(BaseModel):
    """Model for listing keys in the key-value store."""

    model_config = ConfigDict(populate_by_name=True)

    count: Annotated[int, Field(alias='count')]
    limit: Annotated[int, Field(alias='limit')]
    is_truncated: Annotated[bool, Field(alias='isTruncated')]
    items: Annotated[list[KeyValueStoreKeyInfo], Field(alias='items', default_factory=list)]
    exclusive_start_key: Annotated[str | None, Field(alias='exclusiveStartKey', default=None)]
    next_exclusive_start_key: Annotated[str | None, Field(alias='nextExclusiveStartKey', default=None)]


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
    """Model for the request queue head."""

    model_config = ConfigDict(populate_by_name=True)

    limit: Annotated[int | None, Field(alias='limit', default=None)]
    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]
    queue_modified_at: Annotated[datetime, Field(alias='queueModifiedAt')]
    items: Annotated[list[Request], Field(alias='items', default_factory=list)]


@docs_group('Data structures')
class RequestQueueHeadWithLocks(RequestQueueHead):
    """Model for request queue head with locks."""

    lock_secs: Annotated[int, Field(alias='lockSecs')]
    queue_has_locked_requests: Annotated[bool | None, Field(alias='queueHasLockedRequests')] = None


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
class BatchRequestsOperationResponse(BaseModel):
    """Response to batch request deletion calls."""

    model_config = ConfigDict(populate_by_name=True)

    processed_requests: Annotated[list[ProcessedRequest], Field(alias='processedRequests')]
    unprocessed_requests: Annotated[list[UnprocessedRequest], Field(alias='unprocessedRequests')]


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
