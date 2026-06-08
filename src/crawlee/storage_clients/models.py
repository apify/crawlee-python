from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Generic

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from pydantic.alias_generators import to_camel
from typing_extensions import TypeVar

from crawlee._types import HttpMethod, JsonSerializable
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import validate_http_url

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

KvsValueType = TypeVar('KvsValueType', default=Any)


@docs_group('Storage data')
class StorageMetadata(BaseModel):
    """Represents the base model for storage metadata.

    It contains common fields shared across all specific storage types.
    """

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, extra='allow', from_attributes=True
    )

    id: str
    """The unique identifier of the storage."""

    name: str | None = None
    """The name of the storage."""

    accessed_at: datetime
    """The timestamp when the storage was last accessed."""

    created_at: datetime
    """The timestamp when the storage was created."""

    modified_at: datetime
    """The timestamp when the storage was last modified."""


@docs_group('Storage data')
class DatasetMetadata(StorageMetadata):
    """Model for a dataset metadata."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

    item_count: int
    """The number of items in the dataset."""


@docs_group('Storage data')
class KeyValueStoreMetadata(StorageMetadata):
    """Model for a key-value store metadata."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )


@docs_group('Storage data')
class RequestQueueMetadata(StorageMetadata):
    """Model for a request queue metadata."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

    had_multiple_clients: bool
    """Indicates whether the queue has been accessed by multiple clients (consumers)."""

    handled_request_count: int
    """The number of requests that have been handled from the queue."""

    pending_request_count: int
    """The number of requests that are still pending in the queue."""

    total_request_count: int
    """The total number of requests that have been added to the queue."""


@docs_group('Storage data')
class KeyValueStoreRecordMetadata(BaseModel):
    """Model for a key-value store record metadata."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

    key: str
    """The key of the record.

    A unique identifier for the record in the key-value store.
    """

    content_type: str
    """The MIME type of the record.

    Describe the format and type of data stored in the record, following the MIME specification.
    """

    size: int | None = None
    """The size of the record in bytes."""


@docs_group('Storage data')
class KeyValueStoreRecord(KeyValueStoreRecordMetadata, Generic[KvsValueType]):
    """Model for a key-value store record."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

    value: KvsValueType
    """The value of the record."""


@docs_group('Storage data')
class DatasetItemsListPage(BaseModel):
    """Model for a single page of dataset items returned from a collection list method."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

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

    # Workaround for Pydantic and type checkers when using Annotated with default_factory
    if TYPE_CHECKING:
        items: Sequence[Mapping[str, JsonSerializable]] = []
        """The list of dataset items returned on this page."""
    else:
        items: Annotated[list[dict], Field(default_factory=list)]
        """The list of dataset items returned on this page."""


@docs_group('Storage data')
class ProcessedRequest(BaseModel):
    """Represents a processed request."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

    id: Annotated[str | None, Field(alias='requestId', default=None)] = None
    """Internal representation of the request by the storage client. Only some clients use id."""

    unique_key: str
    was_already_present: bool
    was_already_handled: bool


@docs_group('Storage data')
class UnprocessedRequest(BaseModel):
    """Represents an unprocessed request."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

    unique_key: str
    url: Annotated[str, BeforeValidator(validate_http_url)]
    method: HttpMethod | None = None


@docs_group('Storage data')
class AddRequestsResponse(BaseModel):
    """Model for a response to add requests to a queue.

    Contains detailed information about the processing results when adding multiple requests
    to a queue. This includes which requests were successfully processed and which ones
    encountered issues during processing.
    """

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, from_attributes=True
    )

    processed_requests: list[ProcessedRequest]
    """Successfully processed requests, including information about whether they were
    already present in the queue and whether they had been handled previously."""

    unprocessed_requests: list[UnprocessedRequest]
    """Requests that could not be processed, typically due to validation errors or other issues."""
