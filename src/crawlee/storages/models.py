# ruff: noqa: TCH003

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar('T')


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


class KeyValueStoreRecordMetadata(BaseModel):
    """Model for a key-value store record metadata."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    content_type: Annotated[str, Field(alias='contentType')]


# class KeyValueStoreRecord(BaseModel):
#     """Model for a key-value store record."""

#     model_config = ConfigDict(populate_by_name=True)

#     key: Annotated[str, Field(alias='key')]
#     value: Annotated[Any, Field(alias='value')]
#     content_type: Annotated[str | None, Field(alias='contentType', default=None)]
#     filename: Annotated[str | None, Field(alias='filename', default=None)]


# class KeyValueStoreRecordInfo(BaseModel):
#     """Model for a key-value store record info."""

#     model_config = ConfigDict(populate_by_name=True)

#     key: Annotated[str, Field(alias='key')]
#     size: Annotated[int, Field(alias='size')]


class ListPage(BaseModel, Generic[T]):
    """A single page of items returned from a list() method.

    Args:
        items: List of returned objects on this page.
        count: Count of the returned objects on this page.
        offset: The offset of the first object specified in the API call.
        limit: The limit on the number of returned objects specified in the API call.
        total: Total number of objects matching the API call criteria.
        desc: Whether the listing is descending or not.
    """

    items: Annotated[list[T], Field(default_factory=list)]
    count: Annotated[int, Field(default=0)]
    offset: Annotated[int, Field(default=0)]
    limit: Annotated[int, Field(default=0)]
    total: Annotated[int, Field(default=0)]
    desc: Annotated[bool, Field(default=False)]
