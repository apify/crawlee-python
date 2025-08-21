from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import JSON, BigInteger, Boolean, ForeignKey, Index, Integer, LargeBinary, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import DateTime, TypeDecorator
from typing_extensions import override

if TYPE_CHECKING:
    from sqlalchemy.engine import Dialect


# This is necessary because unique constraints don't apply to NULL values in SQL.
class NameDefaultNone(TypeDecorator):
    """Custom SQLAlchemy type for handling default name values.

    Converts None values to 'default' on storage and back to None on retrieval.
    """

    impl = String(100)
    cache_ok = True

    @override
    def process_bind_param(self, value: str | None, _dialect: Dialect) -> str:
        """Convert Python value to database value."""
        return 'default' if value is None else value

    @override
    def process_result_value(self, value: str | None, _dialect: Dialect) -> str | None:
        """Convert database value to Python value."""
        return None if value == 'default' else value


class AwareDateTime(TypeDecorator):
    """Custom SQLAlchemy type for timezone-aware datetime handling.

    Ensures all datetime values are timezone-aware by adding UTC timezone to
    naive datetime values from databases that don't store timezone information.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_result_value(self, value: datetime | None, _dialect: Dialect) -> datetime | None:
        """Add UTC timezone to naive datetime values."""
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value


class Base(DeclarativeBase):
    """Base class for all database models for correct type annotations."""


class StorageMetadataDB:
    """Base database model for storage metadata."""

    id: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    """Unique identifier."""

    name: Mapped[str | None] = mapped_column(NameDefaultNone, nullable=False, index=True, unique=True)
    """Human-readable name. None becomes 'default' in database to enforce uniqueness."""

    accessed_at: Mapped[datetime] = mapped_column(AwareDateTime, nullable=False)
    """Last access datetime for usage tracking."""

    created_at: Mapped[datetime] = mapped_column(AwareDateTime, nullable=False)
    """Creation datetime."""

    modified_at: Mapped[datetime] = mapped_column(AwareDateTime, nullable=False)
    """Last modification datetime."""


class DatasetMetadataDB(StorageMetadataDB, Base):
    """Metadata table for datasets."""

    __tablename__ = 'dataset_metadata'

    item_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Number of items in the dataset."""

    # Relationship to dataset items with cascade deletion
    items: Mapped[list[DatasetItemDB]] = relationship(
        back_populates='dataset', cascade='all, delete-orphan', lazy='select'
    )


class RequestQueueMetadataDB(StorageMetadataDB, Base):
    """Metadata table for request queues."""

    __tablename__ = 'request_queue_metadata'

    had_multiple_clients: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    """Flag indicating if multiple clients have accessed this queue."""

    handled_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Number of requests processed."""

    pending_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Number of requests waiting to be processed."""

    total_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Total number of requests ever added to this queue."""

    # Relationship to queue requests with cascade deletion
    requests: Mapped[list[RequestDB]] = relationship(
        back_populates='queue', cascade='all, delete-orphan', lazy='select'
    )
    # Relationship to queue state
    state: Mapped[RequestQueueStateDB] = relationship(
        back_populates='queue', cascade='all, delete-orphan', lazy='select'
    )


class KeyValueStoreMetadataDB(StorageMetadataDB, Base):
    """Metadata table for key-value stores."""

    __tablename__ = 'kvs_metadata'

    # Relationship to store records with cascade deletion
    records: Mapped[list[KeyValueStoreRecordDB]] = relationship(
        back_populates='kvs', cascade='all, delete-orphan', lazy='select'
    )


class KeyValueStoreRecordDB(Base):
    """Records table for key-value stores."""

    __tablename__ = 'kvs_record'

    metadata_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('kvs_metadata.id', ondelete='CASCADE'), primary_key=True, index=True
    )
    """Foreign key to metadata key-value store record."""

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    """The key part of the key-value pair."""

    value: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    """Value stored as binary data to support any content type."""

    content_type: Mapped[str] = mapped_column(String(50), nullable=False)
    """MIME type for proper value deserialization."""

    size: Mapped[int | None] = mapped_column(Integer, nullable=False, default=0)
    """Size of stored value in bytes."""

    # Relationship back to parent store
    kvs: Mapped[KeyValueStoreMetadataDB] = relationship(back_populates='records')


class DatasetItemDB(Base):
    """Items table for datasets."""

    __tablename__ = 'dataset_item'

    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    """Auto-increment primary key preserving insertion order."""

    metadata_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey('dataset_metadata.id', ondelete='CASCADE'),
        index=True,
    )
    """Foreign key to metadata dataset record."""

    data: Mapped[str] = mapped_column(JSON, nullable=False)
    """JSON-serialized item data."""

    # Relationship back to parent dataset
    dataset: Mapped[DatasetMetadataDB] = relationship(back_populates='items')


class RequestDB(Base):
    """Requests table for request queues."""

    __tablename__ = 'request'
    __table_args__ = (
        # Index for efficient SELECT to cache
        Index('idx_queue_handled_seq', 'metadata_id', 'is_handled', 'sequence_number'),
    )

    request_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    """Unique identifier for the request representing the unique_key."""

    metadata_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('request_queue_metadata.id', ondelete='CASCADE'), primary_key=True
    )
    """Foreign key to metadata request queue record."""

    data: Mapped[str] = mapped_column(JSON, nullable=False)
    """JSON-serialized Request object."""

    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    """Ordering sequence: negative for forefront, positive for regular."""

    is_handled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    """Processing status flag."""

    # Relationship back to metadata table
    queue: Mapped[RequestQueueMetadataDB] = relationship(back_populates='requests')


class RequestQueueStateDB(Base):
    """State table for request queues."""

    __tablename__ = 'request_queue_state'

    metadata_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('request_queue_metadata.id', ondelete='CASCADE'), primary_key=True
    )
    """Foreign key to metadata request queue record."""

    sequence_counter: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    """Counter for regular request ordering (positive)."""

    forefront_sequence_counter: Mapped[int] = mapped_column(Integer, nullable=False, default=-1)
    """Counter for forefront request ordering (negative)."""

    # Relationship back to metadata table
    queue: Mapped[RequestQueueMetadataDB] = relationship(back_populates='state')
