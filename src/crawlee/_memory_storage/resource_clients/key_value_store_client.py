from __future__ import annotations

import asyncio
import io
import json
import mimetypes
import os
import pathlib
from datetime import datetime, timezone
from operator import itemgetter
from typing import TYPE_CHECKING, Any, AsyncIterator, TypedDict

import aiofiles
import aioshutil
from aiofiles.os import makedirs
from apify_shared.utils import ignore_docs, is_file_or_bytes, json_dumps

from apify._crypto import crypto_random_object_id
from apify._memory_storage.file_storage_utils import update_metadata
from apify._memory_storage.resource_clients.base_resource_client import BaseResourceClient
from apify._utils import (
    force_remove,
    force_rename,
    guess_file_extension,
    maybe_parse_body,
    raise_on_duplicate_storage,
    raise_on_non_existing_storage,
)
from apify.consts import DEFAULT_API_PARAM_LIMIT, StorageTypes
from apify.log import logger

if TYPE_CHECKING:
    from typing_extensions import NotRequired

    from apify._memory_storage.memory_storage_client import MemoryStorageClient


class KeyValueStoreRecord(TypedDict):
    key: str
    value: Any
    contentType: str | None
    filename: NotRequired[str]


def _filename_from_record(record: KeyValueStoreRecord) -> str:
    if record.get('filename') is not None:
        return record['filename']

    content_type = record.get('contentType')
    if not content_type or content_type == 'application/octet-stream':
        return record['key']

    extension = guess_file_extension(content_type)
    if record['key'].endswith(f'.{extension}'):
        return record['key']

    return f'{record["key"]}.{extension}'


@ignore_docs
class KeyValueStoreClient(BaseResourceClient):
    """Sub-client for manipulating a single key-value store."""

    _id: str
    _resource_directory: str
    _memory_storage_client: MemoryStorageClient
    _name: str | None
    _records: dict[str, KeyValueStoreRecord]
    _created_at: datetime
    _accessed_at: datetime
    _modified_at: datetime
    _file_operation_lock: asyncio.Lock

    def __init__(
        self: KeyValueStoreClient,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
    ) -> None:
        """Initialize the KeyValueStoreClient."""
        self._id = id or crypto_random_object_id()
        self._resource_directory = os.path.join(base_storage_directory, name or self._id)
        self._memory_storage_client = memory_storage_client
        self._name = name
        self._records = {}
        self._created_at = datetime.now(timezone.utc)
        self._accessed_at = datetime.now(timezone.utc)
        self._modified_at = datetime.now(timezone.utc)
        self._file_operation_lock = asyncio.Lock()

    async def get(self: KeyValueStoreClient) -> dict | None:
        """Retrieve the key-value store.

        Returns:
            dict, optional: The retrieved key-value store, or None if it does not exist
        """
        found = self._find_or_create_client_by_id_or_name(memory_storage_client=self._memory_storage_client, id=self._id, name=self._name)

        if found:
            async with found._file_operation_lock:
                await found._update_timestamps(has_been_modified=False)
                return found._to_resource_info()

        return None

    async def update(self: KeyValueStoreClient, *, name: str | None = None) -> dict:
        """Update the key-value store with specified fields.

        Args:
            name (str, optional): The new name for key-value store

        Returns:
            dict: The updated key-value store
        """
        # Check by id
        existing_store_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self._id)

        # Skip if no changes
        if name is None:
            return existing_store_by_id._to_resource_info()

        async with existing_store_by_id._file_operation_lock:
            # Check that name is not in use already
            existing_store_by_name = next(
                (store for store in self._memory_storage_client._key_value_stores_handled if store._name and store._name.lower() == name.lower()),
                None,
            )

            if existing_store_by_name is not None:
                raise_on_duplicate_storage(StorageTypes.KEY_VALUE_STORE, 'name', name)

            existing_store_by_id._name = name

            previous_dir = existing_store_by_id._resource_directory

            existing_store_by_id._resource_directory = os.path.join(self._memory_storage_client._key_value_stores_directory, name)

            await force_rename(previous_dir, existing_store_by_id._resource_directory)

            # Update timestamps
            await existing_store_by_id._update_timestamps(has_been_modified=True)

        return existing_store_by_id._to_resource_info()

    async def delete(self: KeyValueStoreClient) -> None:
        """Delete the key-value store."""
        store = next((store for store in self._memory_storage_client._key_value_stores_handled if store._id == self._id), None)

        if store is not None:
            async with store._file_operation_lock:
                self._memory_storage_client._key_value_stores_handled.remove(store)
                store._records.clear()

                if os.path.exists(store._resource_directory):
                    await aioshutil.rmtree(store._resource_directory)

    async def list_keys(
        self: KeyValueStoreClient,
        *,
        limit: int = DEFAULT_API_PARAM_LIMIT,
        exclusive_start_key: str | None = None,
    ) -> dict:
        """List the keys in the key-value store.

        Args:
            limit (int, optional): Number of keys to be returned. Maximum value is 1000
            exclusive_start_key (str, optional): All keys up to this one (including) are skipped from the result

        Returns:
            dict: The list of keys in the key-value store matching the given arguments
        """
        # Check by id
        existing_store_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self._id)

        items = []

        for record in existing_store_by_id._records.values():
            size = len(record['value'])
            items.append(
                {
                    'key': record['key'],
                    'size': size,
                }
            )

        if len(items) == 0:
            return {
                'count': len(items),
                'limit': limit,
                'exclusiveStartKey': exclusive_start_key,
                'isTruncated': False,
                'nextExclusiveStartKey': None,
                'items': items,
            }

        # Lexically sort to emulate the API
        items = sorted(items, key=itemgetter('key'))

        truncated_items = items
        if exclusive_start_key is not None:
            key_pos = next((idx for idx, i in enumerate(items) if i['key'] == exclusive_start_key), None)
            if key_pos is not None:
                truncated_items = items[(key_pos + 1) :]

        limited_items = truncated_items[:limit]

        last_item_in_store = items[-1]
        last_selected_item = limited_items[-1]
        is_last_selected_item_absolutely_last = last_item_in_store == last_selected_item
        next_exclusive_start_key = None if is_last_selected_item_absolutely_last else last_selected_item['key']

        async with existing_store_by_id._file_operation_lock:
            await existing_store_by_id._update_timestamps(has_been_modified=False)

        return {
            'count': len(items),
            'limit': limit,
            'exclusiveStartKey': exclusive_start_key,
            'isTruncated': not is_last_selected_item_absolutely_last,
            'nextExclusiveStartKey': next_exclusive_start_key,
            'items': limited_items,
        }

    async def _get_record_internal(
        self: KeyValueStoreClient,
        key: str,
        as_bytes: bool = False,  # noqa: FBT001, FBT002
    ) -> dict | None:
        # Check by id
        existing_store_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self._id)

        stored_record = existing_store_by_id._records.get(key)

        if stored_record is None:
            return None

        record = {
            'key': stored_record['key'],
            'value': stored_record['value'],
            'contentType': stored_record.get('contentType'),
        }

        if not as_bytes:
            try:
                record['value'] = maybe_parse_body(record['value'], record['contentType'])
            except ValueError:
                logger.exception('Error parsing key-value store record')

        async with existing_store_by_id._file_operation_lock:
            await existing_store_by_id._update_timestamps(has_been_modified=False)

        return record

    async def get_record(self: KeyValueStoreClient, key: str) -> dict | None:
        """Retrieve the given record from the key-value store.

        Args:
            key (str): Key of the record to retrieve

        Returns:
            dict, optional: The requested record, or None, if the record does not exist
        """
        return await self._get_record_internal(key)

    async def get_record_as_bytes(self: KeyValueStoreClient, key: str) -> dict | None:
        """Retrieve the given record from the key-value store, without parsing it.

        Args:
            key (str): Key of the record to retrieve

        Returns:
            dict, optional: The requested record, or None, if the record does not exist
        """
        return await self._get_record_internal(key, as_bytes=True)

    async def stream_record(self: KeyValueStoreClient, _key: str) -> AsyncIterator[dict | None]:
        raise NotImplementedError('This method is not supported in local memory storage.')

    async def set_record(self: KeyValueStoreClient, key: str, value: Any, content_type: str | None = None) -> None:
        """Set a value to the given record in the key-value store.

        Args:
            key (str): The key of the record to save the value to
            value (Any): The value to save into the record
            content_type (str, optional): The content type of the saved value
        """
        # Check by id
        existing_store_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self._id)

        if isinstance(value, io.IOBase):
            raise NotImplementedError('File-like values are not supported in local memory storage')

        if content_type is None:
            if is_file_or_bytes(value):
                content_type = 'application/octet-stream'
            elif isinstance(value, str):
                content_type = 'text/plain; charset=utf-8'
            else:
                content_type = 'application/json; charset=utf-8'

        if 'application/json' in content_type and not is_file_or_bytes(value) and not isinstance(value, str):
            value = json_dumps(value).encode('utf-8')

        async with existing_store_by_id._file_operation_lock:
            await existing_store_by_id._update_timestamps(has_been_modified=True)
            record: KeyValueStoreRecord = {
                'key': key,
                'value': value,
                'contentType': content_type,
            }

            old_record = existing_store_by_id._records.get(key)
            existing_store_by_id._records[key] = record

            if self._memory_storage_client._persist_storage:
                if old_record is not None and _filename_from_record(old_record) != _filename_from_record(record):
                    await existing_store_by_id._delete_persisted_record(old_record)

                await existing_store_by_id._persist_record(record)

    async def _persist_record(self: KeyValueStoreClient, record: KeyValueStoreRecord) -> None:
        store_directory = self._resource_directory
        record_filename = _filename_from_record(record)
        record['filename'] = record_filename

        # Ensure the directory for the entity exists
        await makedirs(store_directory, exist_ok=True)

        # Create files for the record
        record_path = os.path.join(store_directory, record_filename)
        record_metadata_path = os.path.join(store_directory, record_filename + '.__metadata__.json')

        # Convert to bytes if string
        if isinstance(record['value'], str):
            record['value'] = record['value'].encode('utf-8')

        async with aiofiles.open(record_path, mode='wb') as f:
            await f.write(record['value'])

        if self._memory_storage_client._write_metadata:
            async with aiofiles.open(record_metadata_path, mode='wb') as f:
                await f.write(
                    json_dumps(
                        {
                            'key': record['key'],
                            'contentType': record['contentType'],
                        }
                    ).encode('utf-8')
                )

    async def delete_record(self: KeyValueStoreClient, key: str) -> None:
        """Delete the specified record from the key-value store.

        Args:
            key (str): The key of the record which to delete
        """
        # Check by id
        existing_store_by_id = self._find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client, id=self._id, name=self._name
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self._id)

        record = existing_store_by_id._records.get(key)

        if record is not None:
            async with existing_store_by_id._file_operation_lock:
                del existing_store_by_id._records[key]
                await existing_store_by_id._update_timestamps(has_been_modified=True)
                if self._memory_storage_client._persist_storage:
                    await existing_store_by_id._delete_persisted_record(record)

    async def _delete_persisted_record(self: KeyValueStoreClient, record: KeyValueStoreRecord) -> None:
        store_directory = self._resource_directory
        record_filename = _filename_from_record(record)

        # Ensure the directory for the entity exists
        await makedirs(store_directory, exist_ok=True)

        # Create files for the record
        record_path = os.path.join(store_directory, record_filename)
        record_metadata_path = os.path.join(store_directory, record_filename + '.__metadata__.json')

        await force_remove(record_path)
        await force_remove(record_metadata_path)

    def _to_resource_info(self: KeyValueStoreClient) -> dict:
        """Retrieve the key-value store info."""
        return {
            'id': self._id,
            'name': self._name,
            'accessedAt': self._accessed_at,
            'createdAt': self._created_at,
            'modifiedAt': self._modified_at,
            'userId': '1',
        }

    async def _update_timestamps(self: KeyValueStoreClient, has_been_modified: bool) -> None:  # noqa: FBT001
        self._accessed_at = datetime.now(timezone.utc)

        if has_been_modified:
            self._modified_at = datetime.now(timezone.utc)

        kv_store_info = self._to_resource_info()
        await update_metadata(
            data=kv_store_info,
            entity_directory=self._resource_directory,
            write_metadata=self._memory_storage_client._write_metadata,
        )

    @classmethod
    def _get_storages_dir(cls: type[KeyValueStoreClient], memory_storage_client: MemoryStorageClient) -> str:
        return memory_storage_client._key_value_stores_directory

    @classmethod
    def _get_storage_client_cache(
        cls: type[KeyValueStoreClient],
        memory_storage_client: MemoryStorageClient,
    ) -> list[KeyValueStoreClient]:
        return memory_storage_client._key_value_stores_handled

    @classmethod
    def _create_from_directory(
        cls: type[KeyValueStoreClient],
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
    ) -> KeyValueStoreClient:
        created_at = datetime.now(timezone.utc)
        accessed_at = datetime.now(timezone.utc)
        modified_at = datetime.now(timezone.utc)

        store_metadata_path = os.path.join(storage_directory, '__metadata__.json')
        if os.path.exists(store_metadata_path):
            with open(store_metadata_path, encoding='utf-8') as f:
                metadata = json.load(f)
            id = metadata['id']  # noqa: A001
            name = metadata['name']
            created_at = datetime.fromisoformat(metadata['createdAt'])
            accessed_at = datetime.fromisoformat(metadata['accessedAt'])
            modified_at = datetime.fromisoformat(metadata['modifiedAt'])

        new_client = KeyValueStoreClient(
            base_storage_directory=memory_storage_client._key_value_stores_directory,
            memory_storage_client=memory_storage_client,
            id=id,
            name=name,
        )

        # Overwrite internal properties
        new_client._accessed_at = accessed_at
        new_client._created_at = created_at
        new_client._modified_at = modified_at

        # Scan the key value store folder, check each entry in there and parse it as a store record
        for entry in os.scandir(storage_directory):
            if not entry.is_file():
                continue

            # Ignore metadata files on their own
            if entry.name.endswith('__metadata__.json'):
                continue

            with open(os.path.join(storage_directory, entry.name), 'rb') as f:
                file_content = f.read()

            # Try checking if this file has a metadata file associated with it
            metadata = None
            if os.path.exists(os.path.join(storage_directory, entry.name + '.__metadata__.json')):
                with open(os.path.join(storage_directory, entry.name + '.__metadata__.json'), encoding='utf-8') as metadata_file:
                    try:
                        metadata = json.load(metadata_file)
                        assert metadata.get('key') is not None  # noqa: S101
                        assert metadata.get('contentType') is not None  # noqa: S101
                    except Exception:
                        logger.warning(
                            f"""Metadata of key-value store entry "{entry.name}" for store {name or id} could not be parsed."""
                            'The metadata file will be ignored.',
                            exc_info=True,
                        )

            if not metadata:
                content_type, _ = mimetypes.guess_type(entry.name)
                if content_type is None:
                    content_type = 'application/octet-stream'

                metadata = {
                    'key': pathlib.Path(entry.name).stem,
                    'contentType': content_type,
                }

            try:
                maybe_parse_body(file_content, metadata['contentType'])
            except Exception:
                metadata['contentType'] = 'application/octet-stream'
                logger.warning(
                    f"""Key-value store entry "{metadata['key']}" for store {name or id} could not be parsed."""
                    'The entry will be assumed as binary.',
                    exc_info=True,
                )

            new_client._records[metadata['key']] = {
                'key': metadata['key'],
                'contentType': metadata['contentType'],
                'filename': entry.name,
                'value': file_content,
            }

        return new_client
