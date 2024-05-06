from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from typing_extensions import overload

from crawlee.configuration import Configuration
from crawlee.consts import DATASET_LABEL, KEY_VALUE_STORE_LABEL, REQUEST_QUEUE_LABEL
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.storage_client_manager import StorageClientManager

if TYPE_CHECKING:
    from crawlee.base_storage_client.base_storage_client import BaseStorageClient
    from crawlee.base_storage_client.types import ResourceClient, ResourceCollectionClient
    from crawlee.storages.dataset import Dataset
    from crawlee.storages.key_value_store import KeyValueStore
    from crawlee.storages.request_queue import RequestQueue


_creation_lock = asyncio.Lock()
"""Lock for storage creation."""

_cache_by_id: dict[str, Dataset | KeyValueStore | RequestQueue] = {}
"""Cache of storages by ID."""

_cache_by_name: dict[str, Dataset | KeyValueStore | RequestQueue] = {}
"""Cache of storages by name."""


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
    storage_client = StorageClientManager.get_storage_client()
    configuration = configuration or Configuration()

    # Try to restore the storage from cache by ID
    if name:
        cached_storage = _cache_by_name.get(name)

        if isinstance(cached_storage, storage_class):
            return cached_storage  # type: ignore

    # Find out if the storage is a default on memory storage
    is_default_on_memory = False
    if not id and not name:
        if isinstance(storage_client, MemoryStorageClient):
            is_default_on_memory = True
        id = configuration.default_storage_id

    # Try to restore storage from cache by ID
    if id:
        cached_storage = _cache_by_id.get(id)

        if isinstance(cached_storage, storage_class):
            return cached_storage  # type: ignore

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
        _cache_by_id[storage.id] = storage
        if storage.name is not None:
            _cache_by_name[storage.name] = storage

    return storage


def remove_storage_from_cache(id: str | None = None, name: str | None = None) -> None:
    """Remove a storage from cache by ID or name."""
    if id:
        try:
            del _cache_by_id[id]
        except KeyError as exc:
            raise RuntimeError(f'Storage with provided ID was not found ({id}).') from exc

    if name:
        try:
            del _cache_by_name[name]
        except KeyError as exc:
            raise RuntimeError(f'Storage with provided name was not found ({name}).') from exc


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
