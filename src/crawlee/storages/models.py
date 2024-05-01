# ruff: noqa: TCH001, TCH002, TCH003

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from crawlee.request import Request


class BaseStorageMetadata(BaseModel):
    """Base model for storage metadata."""

    model_config = ConfigDict(populate_by_name=True)

    id: Annotated[str, Field(alias='id')]
    name: Annotated[str, Field(alias='name', default='')]
    accessed_at: Annotated[datetime, Field(alias='accessedAt')]
    created_at: Annotated[datetime, Field(alias='createdAt')]
    modified_at: Annotated[datetime, Field(alias='modifiedAt')]


class DatasetMetadata(BaseStorageMetadata):
    """Model for a dataset metadata."""

    model_config = ConfigDict(populate_by_name=True)

    item_count: Annotated[int, Field(alias='itemCount')]


class KeyValueStoreMetadata(BaseStorageMetadata):
    """Model for a key-value store metadata."""

    model_config = ConfigDict(populate_by_name=True)

    user_id: Annotated[str, Field(alias='userId')]


class RequestQueueMetadata(BaseStorageMetadata):
    """Model for a request queue metadata."""

    model_config = ConfigDict(populate_by_name=True)

    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]
    handled_request_count: Annotated[int, Field(alias='handledRequestCount')]
    pending_request_count: Annotated[int, Field(alias='pendingRequestCount')]
    stats: Annotated[dict, Field(alias='stats')]
    total_request_count: Annotated[int, Field(alias='totalRequestCount')]
    user_id: Annotated[str, Field(alias='userId')]
    resource_directory: Annotated[str, Field(alias='resourceDirectory')]


class KeyValueStoreRecord(BaseModel):
    """Model for a key-value store record."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    value: Annotated[Any, Field(alias='value')]
    content_type: Annotated[str | None, Field(alias='contentType', default=None)]
    filename: Annotated[str | None, Field(alias='filename', default=None)]


class KeyValueStoreRecordMetadata(BaseModel):
    """Model for a key-value store record metadata."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    content_type: Annotated[str, Field(alias='contentType')]


class KeyValueStoreKeyInfo(BaseModel):
    """Model for a key-value store key info."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    size: Annotated[int, Field(alias='size')]


class KeyValueStoreListKeysPage(BaseModel):
    """Model for listing keys in the key-value store."""

    model_config = ConfigDict(populate_by_name=True)

    count: Annotated[int, Field(alias='count')]
    limit: Annotated[int, Field(alias='limit')]
    is_truncated: Annotated[bool, Field(alias='isTruncated')]
    items: Annotated[list[KeyValueStoreKeyInfo], Field(alias='items', default_factory=list)]
    exclusive_start_key: Annotated[str | None, Field(alias='exclusiveStartKey', default=None)]
    next_exclusive_start_key: Annotated[str | None, Field(alias='nextExclusiveStartKey', default=None)]


class RequestQueueOperationInfo(BaseModel):
    """Model for the request queue operation info."""

    model_config = ConfigDict(populate_by_name=True)

    request_id: Annotated[str, Field(alias='requestId')]
    request_unique_key: Annotated[str, Field(alias='requestUniqueKey')]
    was_already_present: Annotated[bool, Field(alias='wasAlreadyPresent')]
    was_already_handled: Annotated[bool, Field(alias='wasAlreadyHandled')]


class RequestQueueHeadState(BaseModel):
    """Model for the request queue head state."""

    model_config = ConfigDict(populate_by_name=True)

    was_limit_reached: Annotated[bool, Field(alias='wasLimitReached')]
    prev_limit: Annotated[int, Field(alias='prevLimit')]
    queue_modified_at: Annotated[datetime, Field(alias='queueModifiedAt')]
    query_started_at: Annotated[datetime, Field(alias='queryStartedAt')]
    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]


class RequestQueueHead(BaseModel):
    """Model for the request queue head."""

    model_config = ConfigDict(populate_by_name=True)

    limit: Annotated[int | None, Field(alias='limit', default=None)]
    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]
    queue_modified_at: Annotated[datetime, Field(alias='queueModifiedAt')]
    items: Annotated[list[Request], Field(alias='items', default_factory=list)]


class BaseListPage(BaseModel):
    """Model for a single page of storage items returned from a collection list method.

    Args:
        count: Count of the returned objects on this page.
        offset: The offset of the first object specified in the API call.
        limit: The limit on the number of returned objects specified in the API call.
        total: Total number of objects matching the API call criteria.
        desc: Whether the listing is descending or not.
    """

    model_config = ConfigDict(populate_by_name=True)

    count: Annotated[int, Field(default=0)]
    offset: Annotated[int, Field(default=0)]
    limit: Annotated[int, Field(default=0)]
    total: Annotated[int, Field(default=0)]
    desc: Annotated[bool, Field(default=False)]


class DatasetListPage(BaseListPage):
    """Model for a single page of dataset items returned from a collection list method.

    Args:
        items: List of returned dataset items on this page.
    """

    items: Annotated[list[DatasetMetadata], Field(default_factory=list)]


class KeyValueStoreListPage(BaseListPage):
    """Model for a single page of key-value store items returned from a collection list method.

    Args:
        items: List of returned key-value store items on this page.
    """

    items: Annotated[list[KeyValueStoreMetadata], Field(default_factory=list)]


class RequestQueueListPage(BaseListPage):
    """Model for a single page of request queue items returned from a collection list method.

    Args:
        items: List of returned request queue items on this page.
    """

    items: Annotated[list[RequestQueueMetadata], Field(default_factory=list)]


class DatasetItemsListPage(BaseListPage):
    """Model for a single page of dataset items returned from a collection list method.

    Args:
        items: List of returned dataset items on this page.
    """

    items: Annotated[list[dict], Field(default_factory=list)]
