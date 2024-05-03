from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, ClassVar

from typing_extensions import overload

from crawlee.consts import DATASET_LABEL, KEY_VALUE_STORE_LABEL, REQUEST_QUEUE_LABEL
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.storage_client_manager import StorageClientManager

if TYPE_CHECKING:
    from crawlee.base_storage_client.types import ResourceClient, ResourceCollectionClient
    from crawlee.configuration import Configuration
    from crawlee.storages.dataset import Dataset
    from crawlee.storages.key_value_store import KeyValueStore
    from crawlee.storages.request_queue import RequestQueue


class StorageCreator:
    """A class for creating storages."""

    _creation_lock = asyncio.Lock()
    """Lock for storage creation."""

    _cache_by_id: ClassVar[dict[str, Dataset | KeyValueStore | RequestQueue]] = {}
    """Cache of storages by ID."""

    _cache_by_name: ClassVar[dict[str, Dataset | KeyValueStore | RequestQueue]] = {}
    """Cache of storages by name."""

    @classmethod
    @overload
    async def open_storage(
        cls,
        *,
        storage_class: type[Dataset],
        id: str,
        configuration: Configuration,
        name: str | None = None,
    ) -> Dataset: ...

    @classmethod
    @overload
    async def open_storage(
        cls,
        *,
        storage_class: type[KeyValueStore],
        id: str,
        configuration: Configuration,
        name: str | None = None,
    ) -> KeyValueStore: ...

    @classmethod
    @overload
    async def open_storage(
        cls,
        *,
        storage_class: type[RequestQueue],
        id: str,
        configuration: Configuration,
        name: str | None = None,
    ) -> RequestQueue: ...

    @classmethod
    async def open_storage(
        cls,
        *,
        storage_class: type[Dataset | KeyValueStore | RequestQueue],
        id: str,
        configuration: Configuration,
        name: str | None = None,
    ) -> Dataset | KeyValueStore | RequestQueue:
        """Open a either a new key-value store or restore existing one and return it."""
        if id and name:
            raise ValueError('Only one of `id` or `name` should be provided.')

        storage_client = StorageClientManager.get_storage_client()

        # Try to restore storage from cache by ID
        if id:
            cached_storage = cls._cache_by_id.get(id)

            if isinstance(cached_storage, storage_class):
                return cached_storage  # type: ignore

            if cached_storage is not None:
                raise RuntimeError(f'Unexpected storage type ({type(cached_storage)}) found under ID="{id}".')

        # Try to restore the storage from cache by ID
        if name:
            cached_storage = cls._cache_by_name.get(name)

            if isinstance(cached_storage, storage_class):
                return cached_storage  # type: ignore

            if cached_storage is not None:
                raise RuntimeError(f'Unexpected storage type ({type(cached_storage)}) found under name="{name}".')

        # Purge on start if configured
        if configuration.purge_on_start:
            await storage_client.purge_on_start()

        # Find out if the storage is a default on memory storage
        is_default_on_memory = bool(not id and not name and isinstance(storage_client, MemoryStorageClient))

        # Get resource clients
        resource_client: ResourceClient
        resource_collection_client: ResourceCollectionClient
        if storage_class.LABEL == DATASET_LABEL:
            resource_client = storage_client.dataset(id)
            resource_collection_client = storage_client.datasets()
        elif storage_class.LABEL == KEY_VALUE_STORE_LABEL:
            resource_client = storage_client.key_value_store(id)
            resource_collection_client = storage_client.key_value_stores()
        elif storage_class.LABEL == REQUEST_QUEUE_LABEL:
            resource_client = storage_client.request_queue(id)
            resource_collection_client = storage_client.request_queues()
        else:
            raise ValueError(f'Unknown storage class: {storage_class.LABEL}')

        # Lock and create new storage
        async with cls._creation_lock:
            if id and not is_default_on_memory:
                storage_info = await resource_client.get()
                if not storage_info:
                    raise RuntimeError(f'{storage_class.LABEL} with id "{id}" does not exist!')

            elif is_default_on_memory:
                storage_info = await resource_collection_client.get_or_create(name=name, id=id)

            else:
                storage_info = await resource_collection_client.get_or_create(name=name)

            storage = storage_class(
                id=storage_info.id,
                name=storage_info.name,
                configuration=configuration,
                client=storage_client,
            )

            # Cache the storage by ID and name
            cls._cache_by_id[storage.id] = storage
            if storage.name is not None:
                cls._cache_by_name[storage.name] = storage

        return storage

    @classmethod
    def remove_storage_from_cache(cls, id: str | None = None, name: str | None = None) -> None:
        """Remove a storage from cache by ID or name."""
        if id and cls._cache_by_id:
            try:
                del cls._cache_by_id[id]
            except KeyError as exc:
                raise RuntimeError(f'Storage with provided ID was not found ({id}).') from exc

        if name and cls._cache_by_name:
            try:
                del cls._cache_by_name[name]
            except KeyError as exc:
                raise RuntimeError(f'Storage with provided name was not found ({name}).') from exc
