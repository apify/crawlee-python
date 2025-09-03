from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import JSON, BigInteger, Boolean, ForeignKey, Index, Integer, LargeBinary, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import DateTime, TypeDecorator
from typing_extensions import override

if TYPE_CHECKING:
    from sqlalchemy.engine import Dialect
    from sqlalchemy.types import TypeEngine


# This is necessary because unique constraints don't apply to NULL values in SQL.
class NameDefaultNone(TypeDecorator):
    """Custom SQLAlchemy type for handling default name values.

    Converts None values to 'default' on storage and back to None on retrieval.
    """

    impl = String(100)
    cache_ok = True

    @override
    def process_bind_param(self, value: str | None, dialect: Dialect) -> str:
        """Convert Python value to database value."""
        return 'default' if value is None else value

    @override
    def process_result_value(self, value: str | None, dialect: Dialect) -> str | None:
        """Convert database value to Python value."""
        return None if value == 'default' else value


class AwareDateTime(TypeDecorator):
    """Custom SQLAlchemy type for timezone-aware datetime handling.

    Ensures all datetime values are timezone-aware by adding UTC timezone to
    naive datetime values from databases that don't store timezone information.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    @override
    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        """Add UTC timezone to naive datetime values."""
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value


class JsonField(TypeDecorator):
    """Uses JSONB for PostgreSQL and JSON for other databases."""

    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[JSON | JSONB]:
        """Load the appropriate dialect implementation for the JSON type."""
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(JSON())


class Base(DeclarativeBase):
    """Base class for all database models for correct type annotations."""


class StorageMetadataDb:
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


class DatasetMetadataDb(StorageMetadataDb, Base):
    """Metadata table for datasets."""

    __tablename__ = 'datasets'

    item_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Number of items in the dataset."""

    # Relationship to dataset items with cascade deletion
    items: Mapped[list[DatasetItemDb]] = relationship(
        back_populates='dataset', cascade='all, delete-orphan', lazy='noload'
    )


class RequestQueueMetadataDb(StorageMetadataDb, Base):
    """Metadata table for request queues."""

    __tablename__ = 'request_queues'

    had_multiple_clients: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    """Flag indicating if multiple clients have accessed this queue."""

    handled_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Number of requests processed."""

    pending_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Number of requests waiting to be processed."""

    total_request_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Total number of requests ever added to this queue."""

    # Relationship to queue requests with cascade deletion
    requests: Mapped[list[RequestDb]] = relationship(
        back_populates='queue', cascade='all, delete-orphan', lazy='noload'
    )
    # Relationship to queue state
    state: Mapped[RequestQueueStateDb] = relationship(
        back_populates='queue', cascade='all, delete-orphan', lazy='noload'
    )


class KeyValueStoreMetadataDb(StorageMetadataDb, Base):
    """Metadata table for key-value stores."""

    __tablename__ = 'key_value_stores'

    # Relationship to store records with cascade deletion
    records: Mapped[list[KeyValueStoreRecordDb]] = relationship(
        back_populates='kvs', cascade='all, delete-orphan', lazy='noload'
    )


class KeyValueStoreRecordDb(Base):
    """Records table for key-value stores."""

    __tablename__ = 'key_value_store_records'

    metadata_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('key_value_stores.id', ondelete='CASCADE'), primary_key=True, index=True
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
    kvs: Mapped[KeyValueStoreMetadataDb] = relationship(back_populates='records')


class DatasetItemDb(Base):
    """Items table for datasets."""

    __tablename__ = 'dataset_records'

    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    """Auto-increment primary key preserving insertion order."""

    metadata_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey('datasets.id', ondelete='CASCADE'),
        index=True,
    )
    """Foreign key to metadata dataset record."""

    data: Mapped[list[dict[str, Any]] | dict[str, Any]] = mapped_column(JsonField, nullable=False)
    """JSON serializable item data."""

    # Relationship back to parent dataset
    dataset: Mapped[DatasetMetadataDb] = relationship(back_populates='items')


class RequestDb(Base):
    """Requests table for request queues."""

    __tablename__ = 'request_queue_records'
    __table_args__ = (
        Index('idx_fetch_available', 'metadata_id', 'is_handled', 'time_blocked_until', 'sequence_number'),
    )

    request_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    """Unique identifier for the request representing the unique_key."""

    metadata_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('request_queues.id', ondelete='CASCADE'), primary_key=True
    )
    """Foreign key to metadata request queue record."""

    data: Mapped[str] = mapped_column(String, nullable=False)
    """JSON-serialized Request object."""

    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    """Ordering sequence: negative for forefront, positive for regular."""

    is_handled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    """Processing status flag."""

    time_blocked_until: Mapped[datetime | None] = mapped_column(AwareDateTime, nullable=True)
    """Timestamp until which this request is considered blocked for processing by other clients."""

    # Relationship back to metadata table
    queue: Mapped[RequestQueueMetadataDb] = relationship(back_populates='requests')


class RequestQueueStateDb(Base):
    """State table for request queues."""

    __tablename__ = 'request_queue_state'

    metadata_id: Mapped[str] = mapped_column(
        String(20), ForeignKey('request_queues.id', ondelete='CASCADE'), primary_key=True
    )
    """Foreign key to metadata request queue record."""

    sequence_counter: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    """Counter for regular request ordering (positive)."""

    forefront_sequence_counter: Mapped[int] = mapped_column(Integer, nullable=False, default=-1)
    """Counter for forefront request ordering (negative)."""

    # Relationship back to metadata table
    queue: Mapped[RequestQueueMetadataDb] = relationship(back_populates='state')


class VersionDb(Base):
    """Table for storing the database schema version."""

    __tablename__ = 'version'

    version: Mapped[str] = mapped_column(String(10), nullable=False, primary_key=True)
