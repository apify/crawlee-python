from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, overload

from crawlee._utils.crypto import crypto_random_object_id

from ._utils import await_redis_response, read_lua_script

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline
    from redis.commands.core import AsyncScript
    from typing_extensions import NotRequired, Self

    from crawlee.storage_clients.models import DatasetMetadata, KeyValueStoreMetadata, RequestQueueMetadata


logger = getLogger(__name__)


class MetadataUpdateParams(TypedDict, total=False):
    """Parameters for updating metadata."""

    update_accessed_at: NotRequired[bool]
    update_modified_at: NotRequired[bool]


class RedisClientMixin:
    """Mixin class for Redis clients.

    This mixin provides common Redis operations and basic methods for Redis storage clients.
    """

    _DEFAULT_NAME = 'default'
    """Default storage name in key prefix when none provided."""

    _MAIN_KEY: ClassVar[str]
    """Main Redis key prefix for this storage type."""

    _CLIENT_TYPE: ClassVar[str]
    """Human-readable client type for error messages."""

    def __init__(self, storage_name: str, storage_id: str, redis: Redis) -> None:
        self._storage_name = storage_name
        self._storage_id = storage_id
        self._redis = redis

        self._scripts_loaded = False

    @property
    def redis(self) -> Redis:
        """Return the Redis client instance."""
        return self._redis

    @property
    def metadata_key(self) -> str:
        """Return the Redis key for the metadata of this storage."""
        return f'{self._MAIN_KEY}:{self._storage_name}:metadata'

    @classmethod
    async def _get_metadata_by_name(cls, name: str, redis: Redis, *, with_wait: bool = False) -> dict | None:
        """Retrieve metadata by storage name.

        Args:
            name: The name of the storage.
            redis: The Redis client instance.
            with_wait: Whether to wait for the storage to be created if it doesn't exist.
        """
        if with_wait:
            # Wait for the creation signal (max 30 seconds)
            await await_redis_response(redis.blpop([f'{cls._MAIN_KEY}:{name}:created_signal'], timeout=30))
            # Signal consumed, push it back for other waiters
            await await_redis_response(redis.lpush(f'{cls._MAIN_KEY}:{name}:created_signal', 1))

        response = await await_redis_response(redis.json().get(f'{cls._MAIN_KEY}:{name}:metadata'))
        data = response[0] if response is not None and isinstance(response, list) else response
        if data is not None and not isinstance(data, dict):
            raise TypeError('The metadata data was received in an incorrect format.')
        return data

    @classmethod
    async def _get_metadata_name_by_id(cls, id: str, redis: Redis) -> str | None:
        """Retrieve storage name by ID from id_to_name index.

        Args:
            id: The ID of the storage.
            redis: The Redis client instance.
        """
        name = await await_redis_response(redis.hget(f'{cls._MAIN_KEY}:id_to_name', id))
        if isinstance(name, str) or name is None:
            return name
        if isinstance(name, bytes):
            return name.decode('utf-8')
        return None

    @classmethod
    async def _open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        metadata_model: type[DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata],
        redis: Redis,
        extra_metadata_fields: dict[str, Any],
        instance_kwargs: dict[str, Any],
    ) -> Self:
        """Open or create a new Redis storage client.

        Args:
            id: The ID of the storage. If not provided, a random ID will be generated.
            name: The name of the storage for named (global scope) storages.
            alias: The alias of the storage for unnamed (run scope) storages.
            redis: Redis client instance.
            metadata_model: Pydantic model for metadata validation.
            extra_metadata_fields: Storage-specific metadata fields.
            instance_kwargs: Additional arguments for the client constructor.

        Returns:
            An instance for the opened or created storage client.
        """
        internal_name = name or alias or cls._DEFAULT_NAME
        storage_id: str | None = None
        # Determine if storage exists by ID or name
        if id:
            storage_name = await cls._get_metadata_name_by_id(id=id, redis=redis)
            storage_id = id
            if storage_name is None:
                raise ValueError(f'{cls._CLIENT_TYPE} with ID "{id}" does not exist.')
        else:
            metadata_data = await cls._get_metadata_by_name(name=internal_name, redis=redis)
            storage_name = internal_name if metadata_data is not None else None
            storage_id = metadata_data['id'] if metadata_data is not None else None
        # If both storage_name and storage_id are found, open existing storage
        if storage_name and storage_id:
            client = cls(storage_name=storage_name, storage_id=storage_id, redis=redis, **instance_kwargs)
            async with client._get_pipeline() as pipe:
                await client._update_metadata(pipe, update_accessed_at=True)
        # Otherwise, create a new storage
        else:
            now = datetime.now(timezone.utc)
            metadata = metadata_model(
                id=crypto_random_object_id(),
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
                **extra_metadata_fields,
            )
            client = cls(storage_name=internal_name, storage_id=metadata.id, redis=redis, **instance_kwargs)
            created = await client._create_metadata_and_storage(internal_name, metadata.model_dump())
            # The client was probably not created due to a race condition. Let's try to open it using the name.
            if not created:
                metadata_data = await cls._get_metadata_by_name(name=internal_name, redis=redis, with_wait=True)
                client = cls(storage_name=internal_name, storage_id=metadata.id, redis=redis, **instance_kwargs)

        # Ensure Lua scripts are loaded
        await client._ensure_scripts_loaded()
        return client

    async def _load_scripts(self) -> None:
        """Load Lua scripts in Redis."""
        return

    async def _ensure_scripts_loaded(self) -> None:
        """Ensure Lua scripts are loaded in Redis."""
        if not self._scripts_loaded:
            await self._load_scripts()
            self._scripts_loaded = True

    @asynccontextmanager
    async def _get_pipeline(self, *, with_execute: bool = True) -> AsyncIterator[Pipeline]:
        """Create a new Redis pipeline."""
        async with self._redis.pipeline() as pipe:
            try:
                pipe.multi()  # type: ignore[no-untyped-call]
                yield pipe
            finally:
                if with_execute:
                    await pipe.execute()

    async def _create_storage(self, pipeline: Pipeline) -> None:
        """Create the actual storage structure in Redis."""
        _ = pipeline  # To avoid unused variable mypy error

    async def _create_script(self, script_name: str) -> AsyncScript:
        """Load a Lua script from a file and return a Script object."""
        script_content = await asyncio.to_thread(read_lua_script, script_name)

        return self._redis.register_script(script_content)

    async def _create_metadata_and_storage(self, storage_name: str, metadata: dict) -> bool:
        index_id_to_name = f'{self._MAIN_KEY}:id_to_name'
        index_name_to_id = f'{self._MAIN_KEY}:name_to_id'
        metadata['created_at'] = metadata['created_at'].isoformat()
        metadata['accessed_at'] = metadata['accessed_at'].isoformat()
        metadata['modified_at'] = metadata['modified_at'].isoformat()

        # Try to create name_to_id index entry, if it already exists, return False.
        name_to_id = await await_redis_response(self._redis.hsetnx(index_name_to_id, storage_name, metadata['id']))
        # If name already exists, return False. Probably an attempt at parallel creation.
        if not name_to_id:
            return False

        # Create id_to_name index entry, metadata, and storage structure in a transaction.
        async with self._get_pipeline() as pipe:
            await await_redis_response(pipe.hsetnx(index_id_to_name, metadata['id'], storage_name))
            await await_redis_response(pipe.json().set(self.metadata_key, '$', metadata))
            await await_redis_response(pipe.lpush(f'{self._MAIN_KEY}:{storage_name}:created_signal', 1))

            await self._create_storage(pipe)

        return True

    async def _drop(self, extra_keys: list[str]) -> None:
        async with self._get_pipeline() as pipe:
            await pipe.delete(self.metadata_key)
            await pipe.delete(f'{self._MAIN_KEY}:id_to_name', self._storage_id)
            await pipe.delete(f'{self._MAIN_KEY}:name_to_id', self._storage_name)
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:created_signal')
            for key in extra_keys:
                await pipe.delete(key)

    async def _purge(self, extra_keys: list[str], metadata_kwargs: MetadataUpdateParams) -> None:
        async with self._get_pipeline() as pipe:
            for key in extra_keys:
                await pipe.delete(key)
            await self._update_metadata(pipe, **metadata_kwargs)
            await self._create_storage(pipe)

    @overload
    async def _get_metadata(self, metadata_model: type[DatasetMetadata]) -> DatasetMetadata: ...
    @overload
    async def _get_metadata(self, metadata_model: type[KeyValueStoreMetadata]) -> KeyValueStoreMetadata: ...
    @overload
    async def _get_metadata(self, metadata_model: type[RequestQueueMetadata]) -> RequestQueueMetadata: ...

    async def _get_metadata(
        self, metadata_model: type[DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata]
    ) -> DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata:
        """Retrieve client metadata."""
        metadata_dict = await self._get_metadata_by_name(name=self._storage_name, redis=self._redis)
        if metadata_dict is None:
            raise ValueError(f'{self._CLIENT_TYPE} with name "{self._storage_name}" does not exist.')
        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, update_accessed_at=True)

        return metadata_model.model_validate(metadata_dict)

    async def _specific_update_metadata(self, pipeline: Pipeline, **kwargs: Any) -> None:
        """Pipeline operations storage-specific metadata updates.

        Must be implemented by concrete classes.

        Args:
            pipeline: The Redis pipeline to use for the update.
            **kwargs: Storage-specific update parameters.
        """
        _ = pipeline  # To avoid unused variable mypy error
        _ = kwargs

    async def _update_metadata(
        self,
        pipeline: Pipeline,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
        **kwargs: Any,
    ) -> None:
        """Update storage metadata combining common and specific fields.

        Args:
            pipeline: The Redis pipeline to use for the update.
            update_accessed_at: Whether to update accessed_at timestamp.
            update_modified_at: Whether to update modified_at timestamp.
            **kwargs: Additional arguments for _specific_update_metadata.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            await await_redis_response(
                pipeline.json().set(self.metadata_key, '$.accessed_at', now.isoformat(), nx=False, xx=True)
            )
        if update_modified_at:
            await await_redis_response(
                pipeline.json().set(self.metadata_key, '$.modified_at', now.isoformat(), nx=False, xx=True)
            )

        await self._specific_update_metadata(pipeline, **kwargs)
