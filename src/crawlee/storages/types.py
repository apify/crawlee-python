from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar, Union

T = TypeVar('T')


# Type for representing json-serializable values. It's close enough to the real thing supported
# by json.parse, and the best we can do until mypy supports recursive types. It was suggested
# in a discussion with (and approved by) Guido van Rossum, so I'd consider it correct enough.
JSONSerializable = Union[str, int, float, bool, None, dict[str, Any], list[Any]]


class StorageTypes(str, Enum):
    """Possible Crawlee storage types."""

    DATASET = 'Dataset'
    KEY_VALUE_STORE = 'Key-value store'
    REQUEST_QUEUE = 'Request queue'


@dataclass
class KeyValueStoreRecord:
    """Type definition for a key-value store record."""

    key: str
    value: Any
    content_type: str | None = None
    filename: str | None = None


@dataclass
class KeyValueStoreRecordInfo:
    """Information about a key-value store record."""

    key: str
    size: int


@dataclass
class KeyValueStoreListKeysOutput(Generic[T]):
    """Represents the output of listing keys in the key-value store."""

    count: int
    limit: int
    is_truncated: bool
    items: list[T] = field(default_factory=list)
    exclusive_start_key: str | None = None
    next_exclusive_start_key: str | None = None
