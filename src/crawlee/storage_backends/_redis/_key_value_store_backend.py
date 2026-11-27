from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from redis.exceptions import RedisError
from typing_extensions import override

from crawlee._utils.retry import retry_on_error
from crawlee.storage_backends._base import KeyValueStoreBackend
from crawlee.storage_backends.models import (
    KeyValueStoreKeysListPage,
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
)

from ._backend_mixin import MetadataUpdateParams, RedisBackendMixin
from ._utils import await_redis_response, decode_strings, expect_bytes

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from ._storage_backend import RedisStorageBackend

logger = getLogger(__name__)


class RedisKeyValueStoreBackend(KeyValueStoreBackend, RedisBackendMixin):
    """Redis implementation of the key-value store backend.

    This backend persists key-value data to Redis using hash data structures for efficient storage and retrieval.

    The key-value store data is stored in Redis using the following key pattern:
    - `key_value_stores:{name}:items` - Redis hash containing key-value pairs (values stored as binary data).
    - `key_value_stores:{name}:metadata_items` - Redis hash containing metadata for each key.
    - `key_value_stores:{name}:keys` - Redis sorted set of the record keys, which lists them in order page by page.
    - `key_value_stores:{name}:metadata` - Redis JSON object containing store metadata.

    The values are stored as the bytes `KeyValueStore` serializes them to, and the content type and the size of each
    record are kept in the metadata hash.

    All operations are atomic through Redis hash operations and pipeline transactions. The backend supports
    concurrent access through Redis's built-in atomic operations for hash fields.

    Values are stored as raw bytes, so the Redis client must not decode responses. A Redis client created with
    `decode_responses=True` is rejected when a value is read.
    """

    _DEFAULT_NAME = 'default'
    """Default Key-Value Store name key prefix when none provided."""

    _MAIN_KEY = 'key_value_stores'
    """Main Redis key prefix for Key-Value Store."""

    _STORAGE_TYPE = 'Key-value store'
    """Human-readable storage type for error messages."""

    def __init__(self, storage_name: str, storage_id: str, storage_backend: RedisStorageBackend) -> None:
        """Initialize a new instance.

        Preferably use the `RedisKeyValueStoreBackend.open` class method to create a new instance.
        """
        super().__init__(storage_name=storage_name, storage_id=storage_id, storage_backend=storage_backend)

    @property
    def _items_key(self) -> str:
        """Return the Redis key for the items of KVS."""
        return f'{self._MAIN_KEY}:{self._storage_name}:items'

    @property
    def _metadata_items_key(self) -> str:
        """Return the Redis key for the items metadata of KVS."""
        return f'{self._MAIN_KEY}:{self._storage_name}:metadata_items'

    @property
    def _keys_key(self) -> str:
        """Return the Redis key for the sorted set with the record keys of KVS."""
        return f'{self._MAIN_KEY}:{self._storage_name}:keys'

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        storage_backend: RedisStorageBackend,
    ) -> RedisKeyValueStoreBackend:
        """Open or create a new Redis key-value store backend.

        This method attempts to open an existing key-value store from the Redis database. If a store with the specified
        ID or name exists, it loads the metadata from the database. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store. If not provided, a random ID will be generated.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.
            storage_backend: The storage backend providing the Redis client.

        Returns:
            An instance for the opened or created storage backend.
        """
        backend = await cls._open(
            id=id,
            name=name,
            alias=alias,
            storage_backend=storage_backend,
            metadata_model=KeyValueStoreMetadata,
            extra_metadata_fields={},
            instance_kwargs={},
        )
        await backend._index_unindexed_keys()  # noqa: SLF001
        return backend

    @retry_on_error(RedisError)
    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return await self._get_metadata(KeyValueStoreMetadata)

    @retry_on_error(RedisError)
    @override
    async def drop(self) -> None:
        await self._drop(extra_keys=[self._items_key, self._metadata_items_key, self._keys_key])

    @retry_on_error(RedisError)
    @override
    async def purge(self) -> None:
        await self._purge(
            extra_keys=[self._items_key, self._metadata_items_key, self._keys_key],
            metadata_kwargs=MetadataUpdateParams(update_accessed_at=True, update_modified_at=True),
        )

    @retry_on_error(RedisError)
    @override
    async def set_value(self, *, key: str, value: bytes, content_type: str) -> None:
        item_metadata = KeyValueStoreRecordMetadata(
            key=key,
            content_type=content_type,
            size=len(value),
        )
        async with self._get_pipeline() as pipe:
            await await_redis_response(pipe.hset(self._items_key, key, value))

            await await_redis_response(
                pipe.hset(
                    self._metadata_items_key,
                    key,
                    item_metadata.model_dump_json(by_alias=True),
                )
            )
            await await_redis_response(pipe.zadd(self._keys_key, {key: 0}))
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True, update_modified_at=True))

    @retry_on_error(RedisError)
    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        # A single transaction, so the metadata and the value of the record always belong together.
        async with self._get_pipeline(with_execute=False) as pipe:
            await await_redis_response(pipe.hget(self._metadata_items_key, key))
            await await_redis_response(pipe.hget(self._items_key, key))
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True))
            results = await pipe.execute()

        serialized_metadata_item, value = results[0], results[1]

        if serialized_metadata_item is None:
            return None

        metadata_item = KeyValueStoreRecordMetadata.model_validate_json(serialized_metadata_item)
        value_bytes = expect_bytes(value)

        if value_bytes is None:
            logger.warning('Value for key "%s" is missing.', key)
            return None

        return KeyValueStoreRecord(value=value_bytes, **metadata_item.model_dump())

    @retry_on_error(RedisError)
    @override
    async def get_values(self, *, keys: Sequence[str]) -> Mapping[str, KeyValueStoreRecord]:
        keys = list(keys)

        # HMGET requires at least one field.
        if not keys:
            return {}

        # A single transaction, so the metadata and the value of a record always belong together.
        async with self._get_pipeline(with_execute=False) as pipe:
            await await_redis_response(pipe.hmget(self._metadata_items_key, keys))
            await await_redis_response(pipe.hmget(self._items_key, keys))
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True))
            results = await pipe.execute()

        serialized_metadata_items, values = results[0], results[1]

        records = dict[str, KeyValueStoreRecord]()
        for key, serialized_metadata_item, value in zip(keys, serialized_metadata_items, values, strict=True):
            value_bytes = expect_bytes(value)

            # A record deleted after its key was listed is skipped.
            if serialized_metadata_item is None or value_bytes is None:
                continue

            metadata_item = KeyValueStoreRecordMetadata.model_validate_json(serialized_metadata_item)
            records[key] = KeyValueStoreRecord(value=value_bytes, **metadata_item.model_dump())

        return records

    @retry_on_error(RedisError)
    @override
    async def delete_value(self, *, key: str) -> None:
        async with self._get_pipeline() as pipe:
            await await_redis_response(pipe.hdel(self._items_key, key))
            await await_redis_response(pipe.hdel(self._metadata_items_key, key))
            await await_redis_response(pipe.zrem(self._keys_key, key))
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True, update_modified_at=True))

    @retry_on_error(RedisError)
    @override
    async def list_keys(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> KeyValueStoreKeysListPage:
        # All members of the sorted set have the same score, so Redis orders them by their bytes, and a range of
        # them is read without loading the other keys.
        range_start = b'-'
        range_end = b'+'

        if prefix is not None:
            range_start = b'[' + prefix.encode()
            # No UTF-8 encoded key contains the 0xFF byte, so this bound follows every key with the prefix.
            range_end = b'(' + prefix.encode() + b'\xff'

        if exclusive_start_key is not None and (prefix is None or exclusive_start_key >= prefix):
            range_start = b'(' + exclusive_start_key.encode()

        # One key over the limit shows whether more keys follow.
        indexed_keys = await await_redis_response(
            self.redis.zrangebylex(self._keys_key, range_start, range_end, start=0, num=limit + 1)
        )
        keys = decode_strings(indexed_keys)

        is_truncated = len(keys) > limit
        keys = keys[:limit]

        async with self._get_pipeline(with_execute=False) as pipe:
            # HMGET requires at least one field.
            if keys:
                await await_redis_response(pipe.hmget(self._metadata_items_key, keys))
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True))
            results = await pipe.execute()

        serialized_metadata_items = results[0] if keys else []

        # A record deleted after its key was listed has no metadata, and is skipped.
        items = [
            KeyValueStoreRecordMetadata.model_validate_json(serialized_metadata_item)
            for serialized_metadata_item in serialized_metadata_items
            if serialized_metadata_item is not None
        ]

        return KeyValueStoreKeysListPage(
            items=items,
            count=len(items),
            limit=limit,
            exclusive_start_key=exclusive_start_key,
            next_exclusive_start_key=keys[-1] if is_truncated else None,
        )

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for Redis key-value stores.')

    @retry_on_error(RedisError)
    @override
    async def record_exists(self, *, key: str) -> bool:
        async with self._get_pipeline(with_execute=False) as pipe:
            await await_redis_response(pipe.hexists(self._items_key, key))
            await self._update_metadata(
                pipe,
                **MetadataUpdateParams(update_accessed_at=True),
            )
            results = await pipe.execute()

        return bool(results[0])

    async def _index_unindexed_keys(self) -> None:
        """Add the keys of the existing records to the sorted set, for a store written before the set existed."""
        if await await_redis_response(self.redis.exists(self._keys_key)):
            return

        stored_keys = await await_redis_response(self.redis.hkeys(self._metadata_items_key))
        keys = decode_strings(stored_keys)
        if keys:
            await await_redis_response(self.redis.zadd(self._keys_key, dict.fromkeys(keys, 0)))
