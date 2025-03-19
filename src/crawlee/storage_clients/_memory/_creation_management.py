from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import pathlib
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING

from crawlee._consts import METADATA_FILENAME
from crawlee._utils.data_processing import maybe_parse_body
from crawlee._utils.file import json_dumps
from crawlee.storage_clients.models import (
    DatasetMetadata,
    InternalRequest,
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
    RequestQueueMetadata,
)

if TYPE_CHECKING:
    from ._dataset_client import DatasetClient
    from ._key_value_store_client import KeyValueStoreClient
    from ._memory_storage_client import MemoryStorageClient, TResourceClient
    from ._request_queue_client import RequestQueueClient

logger = getLogger(__name__)


async def persist_metadata_if_enabled(*, data: dict, entity_directory: str, write_metadata: bool) -> None:
    """Update or writes metadata to a specified directory.

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
    await asyncio.to_thread(os.makedirs, entity_directory, exist_ok=True)

    # Write the metadata to the file
    file_path = os.path.join(entity_directory, METADATA_FILENAME)
    f = await asyncio.to_thread(open, file_path, mode='wb')
    try:
        s = await json_dumps(data)
        await asyncio.to_thread(f.write, s.encode('utf-8'))
    finally:
        await asyncio.to_thread(f.close)


def find_or_create_client_by_id_or_name_inner(
    resource_client_class: type[TResourceClient],
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> TResourceClient | None:
    """Locate or create a new storage client based on the given ID or name.

    This method attempts to find a storage client in the memory cache first. If not found,
    it tries to locate a storage directory by name. If still not found, it searches through
    storage directories for a matching ID or name in their metadata. If none exists, and the
    specified ID is 'default', it checks for a default storage directory. If a storage client
    is found or created, it is added to the memory cache. If no storage client can be located or
    created, the method returns None.

    Args:
        resource_client_class: The class of the resource client.
        memory_storage_client: The memory storage client used to store and retrieve storage clients.
        id: The unique identifier for the storage client.
        name: The name of the storage client.

    Raises:
        ValueError: If both id and name are None.

    Returns:
        The found or created storage client, or None if no client could be found or created.
    """
    from ._dataset_client import DatasetClient
    from ._key_value_store_client import KeyValueStoreClient
    from ._request_queue_client import RequestQueueClient

    if id is None and name is None:
        raise ValueError('Either id or name must be specified.')

    # First check memory cache
    found = memory_storage_client.get_cached_resource_client(resource_client_class, id, name)

    if found is not None:
        return found

    storage_path = _determine_storage_path(resource_client_class, memory_storage_client, id, name)

    if not storage_path:
        return None

    # Create from directory if storage path is found
    if issubclass(resource_client_class, DatasetClient):
        resource_client = create_dataset_from_directory(storage_path, memory_storage_client, id, name)
    elif issubclass(resource_client_class, KeyValueStoreClient):
        resource_client = create_kvs_from_directory(storage_path, memory_storage_client, id, name)
    elif issubclass(resource_client_class, RequestQueueClient):
        resource_client = create_rq_from_directory(storage_path, memory_storage_client, id, name)
    else:
        raise TypeError('Invalid resource client class.')

    memory_storage_client.add_resource_client_to_cache(resource_client)
    return resource_client


async def get_or_create_inner(
    *,
    memory_storage_client: MemoryStorageClient,
    storage_client_cache: list[TResourceClient],
    resource_client_class: type[TResourceClient],
    name: str | None = None,
    id: str | None = None,
) -> TResourceClient:
    """Retrieve a named storage, or create a new one when it doesn't exist.

    Args:
        memory_storage_client: The memory storage client.
        storage_client_cache: The cache of storage clients.
        resource_client_class: The class of the storage to retrieve or create.
        name: The name of the storage to retrieve or create.
        id: ID of the storage to retrieve or create.

    Returns:
        The retrieved or newly-created storage.
    """
    # If the name or id is provided, try to find the dataset in the cache
    if name or id:
        found = find_or_create_client_by_id_or_name_inner(
            resource_client_class=resource_client_class,
            memory_storage_client=memory_storage_client,
            name=name,
            id=id,
        )
        if found:
            return found

    # Otherwise, create a new one and add it to the cache
    resource_client = resource_client_class(
        id=id,
        name=name,
        memory_storage_client=memory_storage_client,
    )

    storage_client_cache.append(resource_client)

    # Write to the disk
    await persist_metadata_if_enabled(
        data=resource_client.resource_info.model_dump(),
        entity_directory=resource_client.resource_directory,
        write_metadata=memory_storage_client.write_metadata,
    )

    return resource_client


def create_dataset_from_directory(
    storage_directory: str,
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> DatasetClient:
    from ._dataset_client import DatasetClient

    item_count = 0
    has_seen_metadata_file = False
    created_at = datetime.now(timezone.utc)
    accessed_at = datetime.now(timezone.utc)
    modified_at = datetime.now(timezone.utc)

    # Load metadata if it exists
    metadata_filepath = os.path.join(storage_directory, METADATA_FILENAME)

    if os.path.exists(metadata_filepath):
        has_seen_metadata_file = True
        with open(metadata_filepath, encoding='utf-8') as f:
            json_content = json.load(f)
            resource_info = DatasetMetadata(**json_content)

        id = resource_info.id
        name = resource_info.name
        item_count = resource_info.item_count
        created_at = resource_info.created_at
        accessed_at = resource_info.accessed_at
        modified_at = resource_info.modified_at

    # Load dataset entries
    entries: dict[str, dict] = {}

    for entry in os.scandir(storage_directory):
        if entry.is_file():
            if entry.name == METADATA_FILENAME:
                has_seen_metadata_file = True
                continue

            with open(os.path.join(storage_directory, entry.name), encoding='utf-8') as f:
                entry_content = json.load(f)

            entry_name = entry.name.split('.')[0]
            entries[entry_name] = entry_content

            if not has_seen_metadata_file:
                item_count += 1

    # Create new dataset client
    new_client = DatasetClient(
        memory_storage_client=memory_storage_client,
        id=id,
        name=name,
        created_at=created_at,
        accessed_at=accessed_at,
        modified_at=modified_at,
        item_count=item_count,
    )

    new_client.dataset_entries.update(entries)
    return new_client


def create_kvs_from_directory(
    storage_directory: str,
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> KeyValueStoreClient:
    from ._key_value_store_client import KeyValueStoreClient

    created_at = datetime.now(timezone.utc)
    accessed_at = datetime.now(timezone.utc)
    modified_at = datetime.now(timezone.utc)

    # Load metadata if it exists
    metadata_filepath = os.path.join(storage_directory, METADATA_FILENAME)

    if os.path.exists(metadata_filepath):
        with open(metadata_filepath, encoding='utf-8') as f:
            json_content = json.load(f)
            resource_info = KeyValueStoreMetadata(**json_content)

        id = resource_info.id
        name = resource_info.name
        created_at = resource_info.created_at
        accessed_at = resource_info.accessed_at
        modified_at = resource_info.modified_at

    # Create new KVS client
    new_client = KeyValueStoreClient(
        memory_storage_client=memory_storage_client,
        id=id,
        name=name,
        accessed_at=accessed_at,
        created_at=created_at,
        modified_at=modified_at,
    )

    # Scan the KVS folder, check each entry in there and parse it as a store record
    for entry in os.scandir(storage_directory):
        if not entry.is_file():
            continue

        # Ignore metadata files on their own
        if entry.name.endswith(METADATA_FILENAME):
            continue

        # Try checking if this file has a metadata file associated with it
        record_metadata = None
        record_metadata_filepath = os.path.join(storage_directory, f'{entry.name}.__metadata__.json')

        if os.path.exists(record_metadata_filepath):
            with open(record_metadata_filepath, encoding='utf-8') as metadata_file:
                try:
                    json_content = json.load(metadata_file)
                    record_metadata = KeyValueStoreRecordMetadata(**json_content)

                except Exception:
                    logger.warning(
                        f'Metadata of key-value store entry "{entry.name}" for store {name or id} could '
                        'not be parsed. The metadata file will be ignored.',
                        exc_info=True,
                    )

        if not record_metadata:
            content_type, _ = mimetypes.guess_type(entry.name)
            if content_type is None:
                content_type = 'application/octet-stream'

            record_metadata = KeyValueStoreRecordMetadata(
                key=pathlib.Path(entry.name).stem,
                content_type=content_type,
            )

        with open(os.path.join(storage_directory, entry.name), 'rb') as f:
            file_content = f.read()

        try:
            maybe_parse_body(file_content, record_metadata.content_type)
        except Exception:
            record_metadata.content_type = 'application/octet-stream'
            logger.warning(
                f'Key-value store entry "{record_metadata.key}" for store {name or id} could not be parsed.'
                'The entry will be assumed as binary.',
                exc_info=True,
            )

        new_client.records[record_metadata.key] = KeyValueStoreRecord(
            key=record_metadata.key,
            content_type=record_metadata.content_type,
            filename=entry.name,
            value=file_content,
        )

    return new_client


def create_rq_from_directory(
    storage_directory: str,
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> RequestQueueClient:
    from ._request_queue_client import RequestQueueClient

    created_at = datetime.now(timezone.utc)
    accessed_at = datetime.now(timezone.utc)
    modified_at = datetime.now(timezone.utc)
    handled_request_count = 0
    pending_request_count = 0

    # Load metadata if it exists
    metadata_filepath = os.path.join(storage_directory, METADATA_FILENAME)

    if os.path.exists(metadata_filepath):
        with open(metadata_filepath, encoding='utf-8') as f:
            json_content = json.load(f)
            resource_info = RequestQueueMetadata(**json_content)

        id = resource_info.id
        name = resource_info.name
        created_at = resource_info.created_at
        accessed_at = resource_info.accessed_at
        modified_at = resource_info.modified_at
        handled_request_count = resource_info.handled_request_count
        pending_request_count = resource_info.pending_request_count

    # Load request entries
    entries: dict[str, InternalRequest] = {}

    for entry in os.scandir(storage_directory):
        if entry.is_file():
            if entry.name == METADATA_FILENAME:
                continue

            with open(os.path.join(storage_directory, entry.name), encoding='utf-8') as f:
                content = json.load(f)

            request = InternalRequest(**content)

            entries[request.id] = request

    # Create new RQ client
    new_client = RequestQueueClient(
        memory_storage_client=memory_storage_client,
        id=id,
        name=name,
        accessed_at=accessed_at,
        created_at=created_at,
        modified_at=modified_at,
        handled_request_count=handled_request_count,
        pending_request_count=pending_request_count,
    )

    new_client.requests.update(entries)
    return new_client


def _determine_storage_path(
    resource_client_class: type[TResourceClient],
    memory_storage_client: MemoryStorageClient,
    id: str | None = None,
    name: str | None = None,
) -> str | None:
    storages_dir = memory_storage_client._get_storage_dir(resource_client_class)  # noqa: SLF001
    default_id = memory_storage_client._get_default_storage_id(resource_client_class)  # noqa: SLF001

    # Try to find by name directly from directories
    if name:
        possible_storage_path = os.path.join(storages_dir, name)
        if os.access(possible_storage_path, os.F_OK):
            return possible_storage_path

    # If not found, try finding by metadata
    if os.access(storages_dir, os.F_OK):
        for entry in os.scandir(storages_dir):
            if entry.is_dir():
                metadata_path = os.path.join(entry.path, METADATA_FILENAME)
                if os.access(metadata_path, os.F_OK):
                    with open(metadata_path, encoding='utf-8') as metadata_file:
                        try:
                            metadata = json.load(metadata_file)
                            if (id and metadata.get('id') == id) or (name and metadata.get('name') == name):
                                return entry.path
                        except Exception:
                            logger.warning(
                                f'Metadata of store entry "{entry.name}" for store {name or id} could not be parsed. '
                                'The metadata file will be ignored.',
                                exc_info=True,
                            )

    # Check for default storage directory as a last resort
    if id == default_id:
        possible_storage_path = os.path.join(storages_dir, default_id)
        if os.access(possible_storage_path, os.F_OK):
            return possible_storage_path

    return None
