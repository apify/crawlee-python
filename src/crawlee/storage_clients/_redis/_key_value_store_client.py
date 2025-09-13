from __future__ import annotations

import json
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from ._client_mixin import RedisClientMixin
from ._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline

logger = getLogger(__name__)


class RedisKeyValueStoreClient(KeyValueStoreClient, RedisClientMixin):
    """Memory implementation of the key-value store client.

    This client stores data in memory as Python dictionaries. No data is persisted between
    process runs, meaning all stored data is lost when the program terminates. This implementation
    is primarily useful for testing, development, and short-lived crawler operations where
    persistence is not required.

    The memory implementation provides fast access to data but is limited by available memory and
    does not support data sharing across different processes.
    """

    _DEFAULT_NAME = 'default'

    _MAIN_KEY = 'key-value-store'

    def __init__(
        self,
        dataset_name: str,
        redis: Redis,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryDatasetClient.open` class method to create a new instance.
        """
        super().__init__(storage_name=dataset_name, redis=redis)

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        metadata_dict = await self._get_metadata_by_name(name=self._storage_name, redis=self._redis)
        if metadata_dict is None:
            raise ValueError(f'Dataset with name "{self._storage_name}" does not exist.')
        return KeyValueStoreMetadata.model_validate(metadata_dict)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        redis: Redis,
    ) -> RedisKeyValueStoreClient:
        """Open or create a new Redis dataset client.

        This method creates a new Redis dataset instance. Unlike persistent storage implementations, Redis
        datasets don't check for existing datasets with the same name or ID since all data exists only in memory
        and is lost when the process terminates.

        Args:
            id: The ID of the dataset. If not provided, a random ID will be generated.
            name: The name of the dataset. If not provided, the dataset will be unnamed.
            redis: Redis client instance.

        Returns:
            An instance for the opened or created storage client.
        """
        if id:
            dataset_name = await cls._get_metadata_name_by_id(id=id, redis=redis)
            if dataset_name is None:
                raise ValueError(f'Dataset with ID "{id}" does not exist.')
        else:
            search_name = name or cls._DEFAULT_NAME
            metadata_data = await cls._get_metadata_by_name(name=search_name, redis=redis)
            dataset_name = search_name if metadata_data is not None else None
        if dataset_name:
            client = cls(dataset_name=dataset_name, redis=redis)
            async with client._get_pipeline() as pipe:
                await client._update_metadata(pipe, update_accessed_at=True)
        else:
            now = datetime.now(timezone.utc)
            metadata = KeyValueStoreMetadata(
                id=crypto_random_object_id(),
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
            )
            dataset_name = name or cls._DEFAULT_NAME
            client = cls(dataset_name=dataset_name, redis=redis)
            await client._create_metadata_and_storage(metadata.model_dump())
        return client

    @override
    async def drop(self) -> None:
        storage_id = (await self.get_metadata()).id
        async with self._get_pipeline() as pipe:
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:metadata')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:items')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:metadata_items')
            await pipe.delete(f'{self._MAIN_KEY}:id_to_name:{storage_id}')

    @override
    async def purge(self) -> None:
        async with self._get_pipeline() as pipe:
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:items')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:metadata_items')
            await self._update_metadata(
                pipe,
                update_accessed_at=True,
                update_modified_at=True,
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
            await await_redis_response(pipe.hset(f'{self._MAIN_KEY}:{self._storage_name}:items', key, value_bytes))  # type: ignore[arg-type]

            await await_redis_response(
                pipe.hset(
                    f'{self._MAIN_KEY}:{self._storage_name}:metadata_items',
                    key,
                    item_metadata.model_dump_json(),
                )
            )
            await self._update_metadata(pipe, update_accessed_at=True, update_modified_at=True)

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        serialized_metadata_item = await await_redis_response(
            self._redis.hget(f'{self._MAIN_KEY}:{self._storage_name}:metadata_items', key)
        )

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
            self._redis.hget(f'{self._MAIN_KEY}:{self._storage_name}:items', key)  # type: ignore[arg-type]
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
            await await_redis_response(pipe.hdel(f'{self._MAIN_KEY}:{self._storage_name}:items', key))
            await await_redis_response(pipe.hdel(f'{self._MAIN_KEY}:{self._storage_name}:metadata_items', key))
            await self._update_metadata(pipe, update_accessed_at=True, update_modified_at=True)

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        items_data = await await_redis_response(
            self._redis.hgetall(f'{self._MAIN_KEY}:{self._storage_name}:metadata_items')
        )

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
                update_accessed_at=True,
            )

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for memory key-value stores.')

    @override
    async def record_exists(self, *, key: str) -> bool:
        async with self._get_pipeline(with_execute=False) as pipe:
            await await_redis_response(pipe.hexists(f'{self._MAIN_KEY}:{self._storage_name}:items', key))
            await self._update_metadata(
                pipe,
                update_accessed_at=True,
            )
            results = await pipe.execute()

        return bool(results[0])

    async def _update_metadata(
        self,
        pipeline: Pipeline,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the dataset metadata with current information.

        Args:
            pipeline: The Redis pipeline to use for the update.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        metadata_key = f'{self._MAIN_KEY}:{self._storage_name}:metadata'
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.accessed_at', now.isoformat(), nx=False, xx=True)
            )
        if update_modified_at:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.modified_at', now.isoformat(), nx=False, xx=True)
            )
