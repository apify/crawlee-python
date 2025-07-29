from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

Base = declarative_base()


class StorageMetadataDB:
    """Base database model for storage metadata."""

    id: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    accessed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    modified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DatasetMetadataDB(StorageMetadataDB, Base):  # type: ignore[valid-type,misc]
    __tablename__ = 'dataset_metadata'

    item_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    items: Mapped[list[DatasetItemDB]] = relationship(back_populates='dataset', cascade='all, delete-orphan')


class RequestQueueMetadataDB(StorageMetadataDB, Base):  # type: ignore[valid-type,misc]
    __tablename__ = 'request_queue_metadata'

    had_multiple_clients: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    handled_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pending_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    stats: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default={})
    total_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    requests: Mapped[list[RequestDB]] = relationship(back_populates='queue', cascade='all, delete-orphan')


class KeyValueStoreMetadataDB(StorageMetadataDB, Base):  # type: ignore[valid-type,misc]
    __tablename__ = 'kvs_metadata'

    records: Mapped[list[KeyValueStoreRecordDB]] = relationship(back_populates='kvs', cascade='all, delete-orphan')


class KeyValueStoreRecordDB(Base):  # type: ignore[valid-type,misc]
    """Database model for key-value store records."""

    __tablename__ = 'kvs_record'

    kvs_id: Mapped[str] = mapped_column(String(255), ForeignKey('kvs_metadata.id'), primary_key=True, index=True)

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    content_type: Mapped[str] = mapped_column(String(100), nullable=False)
    size: Mapped[int | None] = mapped_column(Integer, nullable=False, default=0)

    kvs: Mapped[KeyValueStoreMetadataDB] = relationship(back_populates='records')


class DatasetItemDB(Base):  # type: ignore[valid-type,misc]
    """Database model for dataset items."""

    __tablename__ = 'dataset_item'

    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[str] = mapped_column(String(20), ForeignKey('dataset_metadata.id'), index=True)
    data: Mapped[str] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    dataset: Mapped[DatasetMetadataDB] = relationship(back_populates='items')


class RequestDB(Base):  # type: ignore[valid-type,misc]
    """Database model for requests in the request queue."""

    __tablename__ = 'request'

    request_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    queue_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('request_queue_metadata.id'), index=True, primary_key=True
    )

    data: Mapped[str] = mapped_column(JSON, nullable=False)
    unique_key: Mapped[str] = mapped_column(String(512), nullable=False)

    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    is_handled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    queue: Mapped[RequestQueueMetadataDB] = relationship(back_populates='requests')
