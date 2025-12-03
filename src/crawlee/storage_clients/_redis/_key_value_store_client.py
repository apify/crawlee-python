from __future__ import annotations

import json
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from ._client_mixin import MetadataUpdateParams, RedisClientMixin
from ._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis

logger = getLogger(__name__)


class RedisKeyValueStoreClient(KeyValueStoreClient, RedisClientMixin):
    """Redis implementation of the key-value store client.

    This client persists key-value data to Redis using hash data structures for efficient storage and retrieval.
    Keys are mapped to values with automatic content type detection and size tracking for metadata management.

    The key-value store data is stored in Redis using the following key pattern:
    - `key_value_stores:{name}:items` - Redis hash containing key-value pairs (values stored as binary data).
    - `key_value_stores:{name}:metadata_items` - Redis hash containing metadata for each key.
    - `key_value_stores:{name}:metadata` - Redis JSON object containing store metadata.

    Values are serialized based on their type: JSON objects are stored as UTF-8 encoded JSON strings,
    text values as UTF-8 encoded strings, and binary data as-is. The implementation automatically handles
    content type detection and maintains metadata about each record including size and MIME type information.

    All operations are atomic through Redis hash operations and pipeline transactions. The client supports
    concurrent access through Redis's built-in atomic operations for hash fields.
    """

    _DEFAULT_NAME = 'default'
    """Default Key-Value Store name key prefix when none provided."""

    _MAIN_KEY = 'key_value_stores'
    """Main Redis key prefix for Key-Value Store."""

    _CLIENT_TYPE = 'Key-value store'
    """Human-readable client type for error messages."""

    def __init__(self, storage_name: str, storage_id: str, redis: Redis) -> None:
        """Initialize a new instance.

        Preferably use the `RedisKeyValueStoreClient.open` class method to create a new instance.
        """
        super().__init__(storage_name=storage_name, storage_id=storage_id, redis=redis)

    @property
    def _items_key(self) -> str:
        """Return the Redis key for the items of KVS."""
        return f'{self._MAIN_KEY}:{self._storage_name}:items'

    @property
    def _metadata_items_key(self) -> str:
        """Return the Redis key for the items metadata of KVS."""
        return f'{self._MAIN_KEY}:{self._storage_name}:metadata_items'

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        redis: Redis,
    ) -> RedisKeyValueStoreClient:
        """Open or create a new Redis key-value store client.

        This method attempts to open an existing key-value store from the Redis database. If a store with the specified
        ID or name exists, it loads the metadata from the database. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store. If not provided, a random ID will be generated.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.
            redis: Redis client instance.

        Returns:
            An instance for the opened or created storage client.
        """
        return await cls._open(
            id=id,
            name=name,
            alias=alias,
            redis=redis,
            metadata_model=KeyValueStoreMetadata,
            extra_metadata_fields={},
            instance_kwargs={},
        )

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return await self._get_metadata(KeyValueStoreMetadata)

    @override
    async def drop(self) -> None:
        await self._drop(extra_keys=[self._items_key, self._metadata_items_key])

    @override
    async def purge(self) -> None:
        await self._purge(
            extra_keys=[self._items_key, self._metadata_items_key],
            metadata_kwargs=MetadataUpdateParams(update_accessed_at=True, update_modified_at=True),
        )

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        # Special handling for None values
        if value is None:
            content_type = 'application/x-none'  # Special content type to identify None values
            value_bytes = b''
        else:
            content_type = content_type or infer_mime_type(value)

            # Serialize the value to bytes.
            if 'application/json' in content_type:
                value_bytes = json.dumps(value, default=str, ensure_ascii=False).encode('utf-8')
            elif isinstance(value, str):
                value_bytes = value.encode('utf-8')
            elif isinstance(value, (bytes, bytearray)):
                value_bytes = value
            else:
                # Fallback: attempt to convert to string and encode.
                value_bytes = str(value).encode('utf-8')

        size = len(value_bytes)
        item_metadata = KeyValueStoreRecordMetadata(
            key=key,
            content_type=content_type,
            size=size,
        )

        async with self._get_pipeline() as pipe:
            # redis-py typing issue
            await await_redis_response(pipe.hset(self._items_key, key, value_bytes))  # type: ignore[arg-type]

            await await_redis_response(
                pipe.hset(
                    self._metadata_items_key,
                    key,
                    item_metadata.model_dump_json(),
                )
            )
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True, update_modified_at=True))

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        serialized_metadata_item = await await_redis_response(self._redis.hget(self._metadata_items_key, key))

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True))

        if not isinstance(serialized_metadata_item, (str, bytes, bytearray)):
            logger.warning(f'Metadata for key "{key}" is missing or invalid.')
            return None

        metadata_item = KeyValueStoreRecordMetadata.model_validate_json(serialized_metadata_item)

        # Handle None values
        if metadata_item.content_type == 'application/x-none':
            return KeyValueStoreRecord(value=None, **metadata_item.model_dump())

        # Query the record by key
        # redis-py typing issue
        value_bytes: bytes | None = await await_redis_response(
            self._redis.hget(self._items_key, key)  # type: ignore[arg-type]
        )

        if value_bytes is None:
            logger.warning(f'Value for key "{key}" is missing.')
            return None

        # Handle JSON values
        if 'application/json' in metadata_item.content_type:
            try:
                value = json.loads(value_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(f'Failed to decode JSON value for key "{key}"')
                return None
        # Handle text values
        elif metadata_item.content_type.startswith('text/'):
            try:
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f'Failed to decode text value for key "{key}"')
                return None
        # Handle binary values
        else:
            value = value_bytes

        return KeyValueStoreRecord(value=value, **metadata_item.model_dump())

    @override
    async def delete_value(self, *, key: str) -> None:
        async with self._get_pipeline() as pipe:
            await await_redis_response(pipe.hdel(self._items_key, key))
            await await_redis_response(pipe.hdel(self._metadata_items_key, key))
            await self._update_metadata(pipe, **MetadataUpdateParams(update_accessed_at=True, update_modified_at=True))

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        items_data = await await_redis_response(self._redis.hgetall(self._metadata_items_key))

        if not items_data:
            return  # No items to iterate over

        if not isinstance(items_data, dict):
            raise TypeError('The items data was received in an incorrect format.')

        # Get all keys, sorted alphabetically
        keys = sorted(items_data.keys())

        # Apply exclusive_start_key filter if provided
        if exclusive_start_key is not None:
            bytes_exclusive_start_key = exclusive_start_key.encode()
            keys = [k for k in keys if k > bytes_exclusive_start_key]

        # Apply limit if provided
        if limit is not None:
            keys = keys[:limit]

        # Yield metadata for each key
        for key in keys:
            record = items_data[key]
            yield KeyValueStoreRecordMetadata.model_validate_json(record)

        async with self._get_pipeline() as pipe:
            await self._update_metadata(
                pipe,
                **MetadataUpdateParams(update_accessed_at=True),
            )

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for memory key-value stores.')

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
