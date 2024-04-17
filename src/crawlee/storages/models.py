# ruff: noqa: TCH003

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


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
