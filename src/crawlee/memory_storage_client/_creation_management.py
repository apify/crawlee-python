from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Callable, overload

import aiofiles
from aiofiles.os import makedirs

from crawlee._utils.file import json_dumps

if TYPE_CHECKING:
    from crawlee.memory_storage_client.dataset_client import DatasetClient
    from crawlee.memory_storage_client.key_value_store_client import KeyValueStoreClient
    from crawlee.memory_storage_client.memory_storage_client import MemoryStorageClient
    from crawlee.memory_storage_client.request_queue_client import RequestQueueClient
    from crawlee.storages.models import DatasetMetadata, KeyValueStoreMetadata, RequestQueueMetadata


async def persist_metadata_if_enabled(*, data: dict, entity_directory: str, write_metadata: bool) -> None:
    """Updates or writes metadata to a specified directory.

    The function writes a given metadata dictionary to a JSON file within a specified directory.
    The writing process is skipped if `write_metadata` is False. Before writing, it ensures that
    the target directory exists, creating it if necessary.

    Args:
        data: A dictionary containing metadata to be written.
        entity_directory: The directory path where the metadata file should be stored.
        write_metadata: A boolean flag indicating whether the metadata should be written to file.
    """
    # Skip metadata write; ensure directory exists first
    if not write_metadata:
        return

    # Ensure the directory for the entity exists
    await makedirs(entity_directory, exist_ok=True)

    # Write the metadata to the file
    file_path = os.path.join(entity_directory, '__metadata__.json')
    async with aiofiles.open(file_path, mode='wb') as f:
        s = await json_dumps(data)
        await f.write(s.encode('utf-8'))


@overload
def find_or_create_client_by_id_or_name_inner(
    storage_client_cache: list[DatasetClient],
    storages_dir: str,
    create_from_directory: Callable[[str, MemoryStorageClient, str | None, str | None], DatasetClient],
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> DatasetClient | None: ...


@overload
def find_or_create_client_by_id_or_name_inner(
    storage_client_cache: list[KeyValueStoreClient],
    storages_dir: str,
    create_from_directory: Callable[[str, MemoryStorageClient, str | None, str | None], KeyValueStoreClient],
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> KeyValueStoreClient | None: ...


@overload
def find_or_create_client_by_id_or_name_inner(
    storage_client_cache: list[RequestQueueClient],
    storages_dir: str,
    create_from_directory: Callable[[str, MemoryStorageClient, str | None, str | None], RequestQueueClient],
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> RequestQueueClient | None: ...


def find_or_create_client_by_id_or_name_inner(
    storage_client_cache: list[DatasetClient] | list[KeyValueStoreClient] | list[RequestQueueClient],
    storages_dir: str,
    create_from_directory: Callable[
        [str, MemoryStorageClient, str | None, str | None],
        DatasetClient | KeyValueStoreClient | RequestQueueClient,
    ],
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> DatasetClient | KeyValueStoreClient | RequestQueueClient | None:
    """Locates or creates a new storage client based on the given ID or name.

    This method attempts to find a storage client in the memory cache first. If not found,
    it tries to locate a storage directory by name. If still not found, it searches through
    storage directories for a matching ID or name in their metadata. If none exists, and the
    specified ID is 'default', it checks for a default storage directory. If a storage client
    is found or created, it is added to the memory cache. If no storage client can be located or
    created, the method returns None.

    Args:
        storage_client_cache: The cache of storage clients.
        storages_dir: The directory where storage clients are stored.
        create_from_directory: The function to create a storage client from a directory.
        memory_storage_client: The memory storage client used to store and retrieve storage clients.
        id: The unique identifier for the storage client. Defaults to None.
        name: The name of the storage client. Defaults to None.

    Raises:
        ValueError: If both id and name are None.

    Returns:
        The found or created storage client, or None if no client could be found or created.
    """
    if id is None and name is None:
        raise ValueError('Either id or name must be specified.')

    # First check memory cache
    found = next(
        (
            storage_client
            for storage_client in storage_client_cache
            if storage_client.id == id or (storage_client.name and name and storage_client.name.lower() == name.lower())
        ),
        None,
    )

    if found is not None:
        return found

    storage_path = None

    # Try to find by name directly from directories
    if name:
        possible_storage_path = os.path.join(storages_dir, name)
        if os.access(possible_storage_path, os.F_OK):
            storage_path = possible_storage_path

    # If not found, try finding by metadata
    if not storage_path and os.access(storages_dir, os.F_OK):
        for entry in os.scandir(storages_dir):
            if entry.is_dir():
                metadata_path = os.path.join(entry.path, '__metadata__.json')
                if os.access(metadata_path, os.F_OK):
                    with open(metadata_path, encoding='utf-8') as metadata_file:
                        metadata = json.load(metadata_file)
                    if (id and metadata.get('id') == id) or (name and metadata.get('name') == name):
                        storage_path = entry.path
                        break

    # Check for default storage directory as a last resort
    if id == 'default':
        possible_storage_path = os.path.join(storages_dir, id)
        if os.access(possible_storage_path, os.F_OK):
            storage_path = possible_storage_path

    if not storage_path:
        return None

    # Create from directory if found
    resource_client = create_from_directory(storage_path, memory_storage_client, id, name)
    storage_client_cache.append(resource_client)  # type: ignore
    return resource_client


@overload
async def get_or_create_inner(
    *,
    memory_storage_client: MemoryStorageClient,
    base_storage_directory: str,
    storage_client_cache: list[DatasetClient],
    resource_client_class: type[DatasetClient],
    name: str | None = None,
    id: str | None = None,
) -> DatasetMetadata: ...


@overload
async def get_or_create_inner(
    *,
    memory_storage_client: MemoryStorageClient,
    base_storage_directory: str,
    storage_client_cache: list[KeyValueStoreClient],
    resource_client_class: type[KeyValueStoreClient],
    name: str | None = None,
    id: str | None = None,
) -> KeyValueStoreMetadata: ...


@overload
async def get_or_create_inner(
    *,
    memory_storage_client: MemoryStorageClient,
    base_storage_directory: str,
    storage_client_cache: list[RequestQueueClient],
    resource_client_class: type[RequestQueueClient],
    name: str | None = None,
    id: str | None = None,
) -> RequestQueueMetadata: ...


async def get_or_create_inner(
    *,
    memory_storage_client: MemoryStorageClient,
    base_storage_directory: str,
    storage_client_cache: list[DatasetClient] | list[KeyValueStoreClient] | list[RequestQueueClient],
    resource_client_class: type[DatasetClient | KeyValueStoreClient | RequestQueueClient],
    name: str | None = None,
    id: str | None = None,
) -> DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata:
    """Retrieve a named storage, or create a new one when it doesn't exist.

    Args:
        memory_storage_client: The memory storage client.
        base_storage_directory: The base directory where the storage clients are stored.
        storage_client_cache: The cache of storage clients.
        resource_client_class: The class of the storage to retrieve or create.
        name: The name of the storage to retrieve or create.
        id: ID of the storage to retrieve or create

    Returns:
        The retrieved or newly-created storage.
    """
    # If the name or id is provided, try to find the dataset in the cache
    if name or id:
        found = resource_client_class.find_or_create_client_by_id_or_name(
            memory_storage_client=memory_storage_client,
            name=name,
            id=id,
        )
        if found:
            return found.resource_info

    # Otherwise, create a new one and add it to the cache
    resource_client = resource_client_class(
        id=id,
        name=name,
        base_storage_directory=base_storage_directory,
        memory_storage_client=memory_storage_client,
    )

    storage_client_cache.append(resource_client)  # type: ignore

    # Write to the disk
    await persist_metadata_if_enabled(
        data=resource_client.resource_info.model_dump(),
        entity_directory=resource_client.resource_directory,
        write_metadata=memory_storage_client.write_metadata,
    )

    return resource_client.resource_info
