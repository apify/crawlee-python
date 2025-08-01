from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import JSON, Boolean, ForeignKey, Index, Integer, LargeBinary, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import DateTime, TypeDecorator

if TYPE_CHECKING:
    from sqlalchemy.engine import Dialect


class NameDefaultNone(TypeDecorator):
    impl = String(100)
    cache_ok = True

    def process_bind_param(self, value: str | None, _dialect: Dialect) -> str | None:
        return 'default' if value is None else value

    def process_result_value(self, value: str | None, _dialect: Dialect) -> str | None:
        return None if value == 'default' else value


class AwareDateTime(TypeDecorator):
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_result_value(self, value: datetime | None, _dialect: Dialect) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value


class Base(DeclarativeBase):
    """Base class for all database models for correct type annotations."""


class StorageMetadataDB:
    """Base database model for storage metadata."""

    id: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    name: Mapped[str | None] = mapped_column(NameDefaultNone, nullable=False, index=True, unique=True)
    accessed_at: Mapped[datetime] = mapped_column(AwareDateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(AwareDateTime, nullable=False)
    modified_at: Mapped[datetime] = mapped_column(AwareDateTime, nullable=False)


class DatasetMetadataDB(StorageMetadataDB, Base):
    __tablename__ = 'dataset_metadata'

    item_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    items: Mapped[list[DatasetItemDB]] = relationship(
        back_populates='dataset', cascade='all, delete-orphan', lazy='select'
    )


class RequestQueueMetadataDB(StorageMetadataDB, Base):
    __tablename__ = 'request_queue_metadata'

    had_multiple_clients: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    handled_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pending_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    requests: Mapped[list[RequestDB]] = relationship(
        back_populates='queue', cascade='all, delete-orphan', lazy='select'
    )


class KeyValueStoreMetadataDB(StorageMetadataDB, Base):
    __tablename__ = 'kvs_metadata'

    records: Mapped[list[KeyValueStoreRecordDB]] = relationship(
        back_populates='kvs', cascade='all, delete-orphan', lazy='select'
    )


class KeyValueStoreRecordDB(Base):
    """Database model for key-value store records."""

    __tablename__ = 'kvs_record'

    kvs_id: Mapped[str] = mapped_column(
        String(255), ForeignKey('kvs_metadata.id', ondelete='CASCADE'), primary_key=True, index=True
    )
    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    content_type: Mapped[str] = mapped_column(String(100), nullable=False)
    size: Mapped[int | None] = mapped_column(Integer, nullable=False, default=0)

    kvs: Mapped[KeyValueStoreMetadataDB] = relationship(back_populates='records')


class DatasetItemDB(Base):
    """Database model for dataset items."""

    __tablename__ = 'dataset_item'

    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey('dataset_metadata.id', ondelete='CASCADE'),
        index=True,
    )
    data: Mapped[str] = mapped_column(JSON, nullable=False)

    dataset: Mapped[DatasetMetadataDB] = relationship(back_populates='items')


class RequestDB(Base):
    """Database model for requests in the request queue."""

    __tablename__ = 'request'
    __table_args__ = (
        Index('idx_queue_handled_seq', 'queue_id', 'is_handled', 'sequence_number'),
        Index('idx_queue_unique_key', 'queue_id', 'unique_key'),
    )

    request_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    queue_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('request_queue_metadata.id', ondelete='CASCADE'), primary_key=True
    )

    data: Mapped[str] = mapped_column(JSON, nullable=False)
    unique_key: Mapped[str] = mapped_column(String(512), nullable=False)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    is_handled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    queue: Mapped[RequestQueueMetadataDB] = relationship(back_populates='requests')
