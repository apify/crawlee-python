from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Generic

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from typing_extensions import TypeVar

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

    size: Annotated[int | None, Field(alias='size', default=None)] = None
    """The size of the record in bytes."""


@docs_group('Data structures')
class KeyValueStoreRecord(KeyValueStoreRecordMetadata, Generic[KvsValueType]):
    """Model for a key-value store record."""

    model_config = ConfigDict(populate_by_name=True)

    value: Annotated[KvsValueType, Field(alias='value')]
    """The value of the record."""


@docs_group('Data structures')
class DatasetItemsListPage(BaseModel):
    """Model for a single page of dataset items returned from a collection list method."""

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

    items: Annotated[list[dict], Field(default_factory=list)]
    """The list of dataset items returned on this page."""


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
