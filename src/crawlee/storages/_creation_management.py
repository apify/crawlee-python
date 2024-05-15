from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from typing_extensions import overload

from crawlee.configuration import Configuration
from crawlee.consts import DATASET_LABEL, KEY_VALUE_STORE_LABEL, REQUEST_QUEUE_LABEL
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.storage_client_manager import StorageClientManager
from crawlee.storages.dataset import Dataset
from crawlee.storages.key_value_store import KeyValueStore
from crawlee.storages.request_queue import RequestQueue

if TYPE_CHECKING:
    from crawlee.base_storage_client.base_storage_client import BaseStorageClient
    from crawlee.base_storage_client.types import ResourceClient, ResourceCollectionClient

_creation_lock = asyncio.Lock()
"""Lock for storage creation."""

_cache_dataset_by_id: dict[str, Dataset] = {}
_cache_dataset_by_name: dict[str, Dataset] = {}
_cache_kvs_by_id: dict[str, KeyValueStore] = {}
_cache_kvs_by_name: dict[str, KeyValueStore] = {}
_cache_rq_by_id: dict[str, RequestQueue] = {}
_cache_rq_by_name: dict[str, RequestQueue] = {}


def _get_from_cache_by_name(
    storage_class_label: str,
    name: str,
) -> Dataset | KeyValueStore | RequestQueue | None:
    """Try to restore storage from cache by name."""
    if storage_class_label == DATASET_LABEL:
        return _cache_dataset_by_name.get(name)
    if storage_class_label == KEY_VALUE_STORE_LABEL:
        return _cache_kvs_by_name.get(name)
    if storage_class_label == REQUEST_QUEUE_LABEL:
        return _cache_rq_by_name.get(name)
    raise ValueError(f'Unknown storage class: {storage_class_label}')


def _get_from_cache_by_id(
    storage_class_label: str,
    id: str,
) -> Dataset | KeyValueStore | RequestQueue | None:
    """Try to restore storage from cache by ID."""
    if storage_class_label == DATASET_LABEL:
        return _cache_dataset_by_id.get(id)
    if storage_class_label == KEY_VALUE_STORE_LABEL:
        return _cache_kvs_by_id.get(id)
    if storage_class_label == REQUEST_QUEUE_LABEL:
        return _cache_rq_by_id.get(id)
    raise ValueError(f'Unknown storage: {storage_class_label}')


def _add_to_cache_by_name(name: str, storage: Dataset | KeyValueStore | RequestQueue) -> None:
    """Add storage to cache by name."""
    if isinstance(storage, Dataset):
        _cache_dataset_by_name[name] = storage
    elif isinstance(storage, KeyValueStore):
        _cache_kvs_by_name[name] = storage
    elif isinstance(storage, RequestQueue):
        _cache_rq_by_name[name] = storage
    else:
        raise TypeError(f'Unknown storage: {storage}')


def _add_to_cache_by_id(id: str, storage: Dataset | KeyValueStore | RequestQueue) -> None:
    """Add storage to cache by ID."""
    if isinstance(storage, Dataset):
        _cache_dataset_by_id[id] = storage
    elif isinstance(storage, KeyValueStore):
        _cache_kvs_by_id[id] = storage
    elif isinstance(storage, RequestQueue):
        _cache_rq_by_id[id] = storage
    else:
        raise TypeError(f'Unknown storage: {storage}')


def _rm_from_cache_by_id(storage_class_label: str, id: str) -> None:
    """Remove a storage from cache by ID."""
    try:
        if storage_class_label == DATASET_LABEL:
            del _cache_dataset_by_id[id]
        elif storage_class_label == KEY_VALUE_STORE_LABEL:
            del _cache_kvs_by_id[id]
        elif storage_class_label == REQUEST_QUEUE_LABEL:
            del _cache_rq_by_id[id]
        else:
            raise ValueError(f'Unknown storage class: {storage_class_label}')
    except KeyError as exc:
        raise RuntimeError(f'Storage with provided ID was not found ({id}).') from exc


def _rm_from_cache_by_name(storage_class_label: str, name: str) -> None:
    """Remove a storage from cache by name."""
    try:
        if storage_class_label == DATASET_LABEL:
            del _cache_dataset_by_name[name]
        elif storage_class_label == KEY_VALUE_STORE_LABEL:
            del _cache_kvs_by_name[name]
        elif storage_class_label == REQUEST_QUEUE_LABEL:
            del _cache_rq_by_name[name]
        else:
            raise ValueError(f'Unknown storage class: {storage_class_label}')
    except KeyError as exc:
        raise RuntimeError(f'Storage with provided name was not found ({name}).') from exc


@overload
async def open_storage(
    *,
    storage_class: type[Dataset],
    configuration: Configuration | None = None,
    id: str | None = None,
    name: str | None = None,
) -> Dataset: ...


@overload
async def open_storage(
    *,
    storage_class: type[KeyValueStore],
    configuration: Configuration | None = None,
    id: str | None = None,
    name: str | None = None,
) -> KeyValueStore: ...


@overload
async def open_storage(
    *,
    storage_class: type[RequestQueue],
    configuration: Configuration | None = None,
    id: str | None = None,
    name: str | None = None,
) -> RequestQueue: ...


async def open_storage(
    *,
    storage_class: type[Dataset | KeyValueStore | RequestQueue],
    configuration: Configuration | None = None,
    id: str | None = None,
    name: str | None = None,
) -> Dataset | KeyValueStore | RequestQueue:
    """Open a either a new key-value store or restore existing one and return it."""
    configuration = configuration or Configuration()
    storage_client = StorageClientManager.get_storage_client(in_cloud=configuration.in_cloud)

    # Try to restore the storage from cache by ID
    if name:
        cached_storage = _get_from_cache_by_name(storage_class_label=storage_class.LABEL, name=name)
        if cached_storage:
            return cached_storage

    # Find out if the storage is a default on memory storage
    is_default_on_memory = False
    if not id and not name:
        if isinstance(storage_client, MemoryStorageClient):
            is_default_on_memory = True
        id = configuration.default_storage_id

    # Try to restore storage from cache by ID
    if id:
        cached_storage = _get_from_cache_by_id(storage_class_label=storage_class.LABEL, id=id)
        if cached_storage:
            return cached_storage

    # Purge on start if configured
    if configuration.purge_on_start:
        await storage_client.purge_on_start()

    # Lock and create new storage
    async with _creation_lock:
        if id and not is_default_on_memory:
            resource_client = _get_resource_client(storage_class.LABEL, storage_client, id)
            storage_info = await resource_client.get()
            if not storage_info:
                raise RuntimeError(f'{storage_class.LABEL} with id "{id}" does not exist!')

        elif is_default_on_memory:
            resource_collection_client = _get_resource_collection_client(storage_class.LABEL, storage_client)
            storage_info = await resource_collection_client.get_or_create(name=name, id=id)

        else:
            resource_collection_client = _get_resource_collection_client(storage_class.LABEL, storage_client)
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
    storage_class_label: str,
    id: str | None = None,
    name: str | None = None,
) -> None:
    """Remove a storage from cache by ID or name."""
    if id:
        _rm_from_cache_by_id(storage_class_label=storage_class_label, id=id)

    if name:
        _rm_from_cache_by_name(storage_class_label=storage_class_label, name=name)


def _get_resource_client(
    storage_class_label: str,
    storage_client: BaseStorageClient,
    id: str,
) -> ResourceClient:
    if storage_class_label == DATASET_LABEL:
        return storage_client.dataset(id)

    if storage_class_label == KEY_VALUE_STORE_LABEL:
        return storage_client.key_value_store(id)

    if storage_class_label == REQUEST_QUEUE_LABEL:
        return storage_client.request_queue(id)

    raise ValueError(f'Unknown storage class label: {storage_class_label}')


def _get_resource_collection_client(
    storage_class_label: str,
    storage_client: BaseStorageClient,
) -> ResourceCollectionClient:
    if storage_class_label == DATASET_LABEL:
        return storage_client.datasets()

    if storage_class_label == KEY_VALUE_STORE_LABEL:
        return storage_client.key_value_stores()

    if storage_class_label == REQUEST_QUEUE_LABEL:
        return storage_client.request_queues()

    raise ValueError(f'Unknown storage class: {storage_class_label}')
