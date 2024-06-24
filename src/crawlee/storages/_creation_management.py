from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, TypeVar

from crawlee.configuration import Configuration
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.storage_client_manager import StorageClientManager
from crawlee.storages import Dataset, KeyValueStore, RequestQueue

if TYPE_CHECKING:
    from crawlee.base_storage_client import BaseStorageClient
    from crawlee.base_storage_client.types import ResourceClient, ResourceCollectionClient

TResource = TypeVar('TResource', Dataset, KeyValueStore, RequestQueue)


_creation_lock = asyncio.Lock()
"""Lock for storage creation."""

_cache_dataset_by_id: dict[str, Dataset] = {}
_cache_dataset_by_name: dict[str, Dataset] = {}
_cache_kvs_by_id: dict[str, KeyValueStore] = {}
_cache_kvs_by_name: dict[str, KeyValueStore] = {}
_cache_rq_by_id: dict[str, RequestQueue] = {}
_cache_rq_by_name: dict[str, RequestQueue] = {}


def _get_from_cache_by_name(
    storage_class: type[TResource],
    name: str,
) -> TResource | None:
    """Try to restore storage from cache by name."""
    if issubclass(storage_class, Dataset):
        return _cache_dataset_by_name.get(name)  # pyright: ignore
    if issubclass(storage_class, KeyValueStore):
        return _cache_kvs_by_name.get(name)  # pyright: ignore
    if issubclass(storage_class, RequestQueue):
        return _cache_rq_by_name.get(name)  # pyright: ignore
    raise ValueError(f'Unknown storage class: {storage_class.__name__}')


def _get_from_cache_by_id(
    storage_class: type[TResource],
    id: str,
) -> TResource | None:
    """Try to restore storage from cache by ID."""
    if issubclass(storage_class, Dataset):
        return _cache_dataset_by_id.get(id)  # pyright: ignore
    if issubclass(storage_class, KeyValueStore):
        return _cache_kvs_by_id.get(id)  # pyright: ignore
    if issubclass(storage_class, RequestQueue):
        return _cache_rq_by_id.get(id)  # pyright: ignore
    raise ValueError(f'Unknown storage: {storage_class.__name__}')


def _add_to_cache_by_name(name: str, storage: TResource) -> None:
    """Add storage to cache by name."""
    if isinstance(storage, Dataset):
        _cache_dataset_by_name[name] = storage
    elif isinstance(storage, KeyValueStore):
        _cache_kvs_by_name[name] = storage
    elif isinstance(storage, RequestQueue):
        _cache_rq_by_name[name] = storage
    else:
        raise TypeError(f'Unknown storage: {storage}')


def _add_to_cache_by_id(id: str, storage: TResource) -> None:
    """Add storage to cache by ID."""
    if isinstance(storage, Dataset):
        _cache_dataset_by_id[id] = storage
    elif isinstance(storage, KeyValueStore):
        _cache_kvs_by_id[id] = storage
    elif isinstance(storage, RequestQueue):
        _cache_rq_by_id[id] = storage
    else:
        raise TypeError(f'Unknown storage: {storage}')


def _rm_from_cache_by_id(storage_class: type, id: str) -> None:
    """Remove a storage from cache by ID."""
    try:
        if issubclass(storage_class, Dataset):
            del _cache_dataset_by_id[id]
        elif issubclass(storage_class, KeyValueStore):
            del _cache_kvs_by_id[id]
        elif issubclass(storage_class, RequestQueue):
            del _cache_rq_by_id[id]
        else:
            raise TypeError(f'Unknown storage class: {storage_class.__name__}')
    except KeyError as exc:
        raise RuntimeError(f'Storage with provided ID was not found ({id}).') from exc


def _rm_from_cache_by_name(storage_class: type, name: str) -> None:
    """Remove a storage from cache by name."""
    try:
        if issubclass(storage_class, Dataset):
            del _cache_dataset_by_name[name]
        elif issubclass(storage_class, KeyValueStore):
            del _cache_kvs_by_name[name]
        elif issubclass(storage_class, RequestQueue):
            del _cache_rq_by_name[name]
        else:
            raise TypeError(f'Unknown storage class: {storage_class.__name__}')
    except KeyError as exc:
        raise RuntimeError(f'Storage with provided name was not found ({name}).') from exc


def _get_default_storage_id(configuration: Configuration, storage_class: type[TResource]) -> str:
    if issubclass(storage_class, Dataset):
        return configuration.default_dataset_id
    if issubclass(storage_class, KeyValueStore):
        return configuration.default_key_value_store_id
    if issubclass(storage_class, RequestQueue):
        return configuration.default_request_queue_id

    raise TypeError(f'Unknown storage class: {storage_class.__name__}')


async def open_storage(
    *,
    storage_class: type[TResource],
    configuration: Configuration | None = None,
    id: str | None = None,
    name: str | None = None,
) -> TResource:
    """Open either a new storage or restore an existing one and return it."""
    configuration = configuration or Configuration.get_global_configuration()
    storage_client = StorageClientManager.get_storage_client(in_cloud=configuration.in_cloud)

    # Try to restore the storage from cache by name
    if name:
        cached_storage = _get_from_cache_by_name(storage_class=storage_class, name=name)
        if cached_storage:
            return cached_storage

    default_id = _get_default_storage_id(configuration, storage_class)

    if not id and not name:
        id = default_id

    # Find out if the storage is a default on memory storage
    is_default_on_memory = id == default_id and isinstance(storage_client, MemoryStorageClient)

    # Try to restore storage from cache by ID
    if id:
        cached_storage = _get_from_cache_by_id(storage_class=storage_class, id=id)
        if cached_storage:
            return cached_storage

    # Purge on start if configured
    if configuration.purge_on_start:
        await storage_client.purge_on_start()

    # Lock and create new storage
    async with _creation_lock:
        if id and not is_default_on_memory:
            resource_client = _get_resource_client(storage_class, storage_client, id)
            storage_info = await resource_client.get()
            if not storage_info:
                raise RuntimeError(f'{storage_class.__name__} with id "{id}" does not exist!')

        elif is_default_on_memory:
            resource_collection_client = _get_resource_collection_client(storage_class, storage_client)
            storage_info = await resource_collection_client.get_or_create(name=name, id=id)

        else:
            resource_collection_client = _get_resource_collection_client(storage_class, storage_client)
            storage_info = await resource_collection_client.get_or_create(name=name)

        storage = storage_class(
            id=storage_info.id,
            name=storage_info.name,
            configuration=configuration,
            client=storage_client,
        )

        # Cache the storage by ID and name
        _add_to_cache_by_id(storage.id, storage)
        if storage.name is not None:
            _add_to_cache_by_name(storage.name, storage)

    return storage


def remove_storage_from_cache(
    *,
    storage_class: type,
    id: str | None = None,
    name: str | None = None,
) -> None:
    """Remove a storage from cache by ID or name."""
    if id:
        _rm_from_cache_by_id(storage_class=storage_class, id=id)

    if name:
        _rm_from_cache_by_name(storage_class=storage_class, name=name)


def _get_resource_client(
    storage_class: type[TResource],
    storage_client: BaseStorageClient,
    id: str,
) -> ResourceClient:
    if issubclass(storage_class, Dataset):
        return storage_client.dataset(id)

    if issubclass(storage_class, KeyValueStore):
        return storage_client.key_value_store(id)

    if issubclass(storage_class, RequestQueue):
        return storage_client.request_queue(id)

    raise ValueError(f'Unknown storage class label: {storage_class.__name__}')


def _get_resource_collection_client(
    storage_class: type,
    storage_client: BaseStorageClient,
) -> ResourceCollectionClient:
    if issubclass(storage_class, Dataset):
        return storage_client.datasets()

    if issubclass(storage_class, KeyValueStore):
        return storage_client.key_value_stores()

    if issubclass(storage_class, RequestQueue):
        return storage_client.request_queues()

    raise ValueError(f'Unknown storage class: {storage_class.__name__}')
