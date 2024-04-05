from __future__ import annotations

import asyncio
import io
import json
import mimetypes
import os
import pathlib
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, AsyncIterator

import aiofiles
import aioshutil
from aiofiles.os import makedirs
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.data_processing import maybe_parse_body, raise_on_duplicate_storage, raise_on_non_existing_storage
from crawlee._utils.file import (
    determine_file_extension,
    force_remove,
    force_rename,
    is_file_or_bytes,
    json_dumps,
    persist_metadata_if_enabled,
)
from crawlee.resource_clients.base_resource_client import BaseResourceClient
from crawlee.storages.types import (
    KeyValueStoreListKeysOutput,
    KeyValueStoreRecord,
    KeyValueStoreRecordInfo,
    KeyValueStoreResourceInfo,
    StorageTypes,
)

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient

logger = getLogger(__name__)


class KeyValueStoreClient(BaseResourceClient):
    """Sub-client for manipulating a single key-value store."""

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id_: str | None = None,
        name: str | None = None,
        created_at: datetime | None = None,
        accessed_at: datetime | None = None,
        modified_at: datetime | None = None,
    ) -> None:
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client
        self.id = id_ or crypto_random_object_id()
        self.name = name
        self._created_at = created_at or datetime.now(timezone.utc)
        self._accessed_at = accessed_at or datetime.now(timezone.utc)
        self._modified_at = modified_at or datetime.now(timezone.utc)

        self.resource_directory = os.path.join(base_storage_directory, self.name or self.id)
        self.records: dict[str, KeyValueStoreRecord] = {}
        self.file_operation_lock = asyncio.Lock()

    @property
    @override
    def resource_info(self) -> KeyValueStoreResourceInfo:
        """Get the resource info for the key-value store client."""
        return KeyValueStoreResourceInfo(
            id=str(self.id),
            name=str(self.name),
            accessed_at=self._accessed_at,
            created_at=self._created_at,
            modified_at=self._modified_at,
            user_id='1',
        )

    @classmethod
    @override
    def _get_storages_dir(cls, memory_storage_client: MemoryStorageClient) -> str:
        return memory_storage_client.key_value_stores_directory

    @classmethod
    @override
    def _get_storage_client_cache(
        cls,
        memory_storage_client: MemoryStorageClient,
    ) -> list[KeyValueStoreClient]:
        return memory_storage_client.key_value_stores_handled

    @classmethod
    @override
    def _create_from_directory(
        cls,
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id_: str | None = None,
        name: str | None = None,
    ) -> KeyValueStoreClient:
        created_at = datetime.now(timezone.utc)
        accessed_at = datetime.now(timezone.utc)
        modified_at = datetime.now(timezone.utc)

        store_metadata_path = os.path.join(storage_directory, '__metadata__.json')
        if os.path.exists(store_metadata_path):
            with open(store_metadata_path, encoding='utf-8') as f:
                metadata = json.load(f)
            id_ = metadata['id']
            name = metadata['name']
            created_at = datetime.fromisoformat(metadata['createdAt'])
            accessed_at = datetime.fromisoformat(metadata['accessedAt'])
            modified_at = datetime.fromisoformat(metadata['modifiedAt'])

        new_client = KeyValueStoreClient(
            base_storage_directory=memory_storage_client.key_value_stores_directory,
            memory_storage_client=memory_storage_client,
            id_=id_,
            name=name,
            accessed_at=accessed_at,
            created_at=created_at,
            modified_at=modified_at,
        )

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
                with open(
                    os.path.join(storage_directory, entry.name + '.__metadata__.json'), encoding='utf-8'
                ) as metadata_file:
                    try:
                        metadata = json.load(metadata_file)

                        if metadata.get('key') is None:
                            raise ValueError('Metadata missing required "key".')  # noqa: TRY301

                        if metadata.get('contentType') is None:
                            raise ValueError('Metadata missing required "contentType".')  # noqa: TRY301

                    except Exception:
                        logger.warning(
                            f'Metadata of key-value store entry "{entry.name}" for store {name or id} could '
                            'not be parsed. The metadata file will be ignored.',
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

            new_client.records[metadata['key']] = KeyValueStoreRecord(
                key=metadata['key'],
                content_type=metadata['contentType'],
                filename=entry.name,
                value=file_content,
            )

        return new_client

    @override
    async def get(self) -> KeyValueStoreResourceInfo | None:
        found = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id_=self.id,
            name=self.name,
        )

        if found:
            async with found.file_operation_lock:
                await found.update_timestamps(has_been_modified=False)
                return found.resource_info

        return None

    async def update(self, *, name: str | None = None) -> KeyValueStoreResourceInfo:
        """Update the key-value store with specified fields.

        Args:
            name: The new name for key-value store

        Returns:
            The updated key-value store
        """
        # Check by id
        existing_store_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id_=self.id,
            name=self.name,
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self.id)

        # Skip if no changes
        if name is None:
            return existing_store_by_id.resource_info

        async with existing_store_by_id.file_operation_lock:
            # Check that name is not in use already
            existing_store_by_name = next(
                (
                    store
                    for store in self._memory_storage_client.key_value_stores_handled
                    if store.name and store.name.lower() == name.lower()
                ),
                None,
            )

            if existing_store_by_name is not None:
                raise_on_duplicate_storage(StorageTypes.KEY_VALUE_STORE, 'name', name)

            existing_store_by_id.name = name

            previous_dir = existing_store_by_id.resource_directory

            existing_store_by_id.resource_directory = os.path.join(
                self._memory_storage_client.key_value_stores_directory,
                name,
            )

            await force_rename(previous_dir, existing_store_by_id.resource_directory)

            # Update timestamps
            await existing_store_by_id.update_timestamps(has_been_modified=True)

        return existing_store_by_id.resource_info

    async def delete(self) -> None:
        """Delete the key-value store."""
        store = next(
            (store for store in self._memory_storage_client.key_value_stores_handled if store.id == self.id), None
        )

        if store is not None:
            async with store.file_operation_lock:
                self._memory_storage_client.key_value_stores_handled.remove(store)
                store.records.clear()

                if os.path.exists(store.resource_directory):
                    await aioshutil.rmtree(store.resource_directory)

    async def list_keys(
        self,
        *,
        limit: int = 1000,
        exclusive_start_key: str | None = None,
    ) -> KeyValueStoreListKeysOutput:
        """List the keys in the key-value store.

        Args:
            limit: Number of keys to be returned. Maximum value is 1000.
            exclusive_start_key: All keys up to this one (including) are skipped from the result.

        Returns:
            The list of keys in the key-value store matching the given arguments.
        """
        # Check by id
        existing_store_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id_=self.id,
            name=self.name,
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self.id)

        items: list[KeyValueStoreRecordInfo] = []

        for record in existing_store_by_id.records.values():
            size = len(record.value)
            items.append(KeyValueStoreRecordInfo(key=record.key, size=size))

        if len(items) == 0:
            return KeyValueStoreListKeysOutput(
                count=len(items),
                limit=limit,
                exclusive_start_key=exclusive_start_key,
                is_truncated=False,
                next_exclusive_start_key=None,
                items=items,
            )

        # Lexically sort to emulate the API
        items = sorted(items, key=lambda item: item.key)

        truncated_items = items
        if exclusive_start_key is not None:
            key_pos = next((idx for idx, item in enumerate(items) if item.key == exclusive_start_key), None)
            if key_pos is not None:
                truncated_items = items[(key_pos + 1) :]

        limited_items = truncated_items[:limit]

        last_item_in_store = items[-1]
        last_selected_item = limited_items[-1]
        is_last_selected_item_absolutely_last = last_item_in_store == last_selected_item
        next_exclusive_start_key = None if is_last_selected_item_absolutely_last else last_selected_item.key

        async with existing_store_by_id.file_operation_lock:
            await existing_store_by_id.update_timestamps(has_been_modified=False)

        return KeyValueStoreListKeysOutput(
            count=len(items),
            limit=limit,
            exclusive_start_key=exclusive_start_key,
            is_truncated=not is_last_selected_item_absolutely_last,
            next_exclusive_start_key=next_exclusive_start_key,
            items=limited_items,
        )

    async def _get_record_internal(
        self,
        key: str,
        *,
        as_bytes: bool = False,
    ) -> KeyValueStoreRecord | None:
        # Check by id
        existing_store_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id_=self.id,
            name=self.name,
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self.id)

        stored_record = existing_store_by_id.records.get(key)

        if stored_record is None:
            return None

        record = KeyValueStoreRecord(
            key=stored_record.key,
            value=stored_record.value,
            content_type=stored_record.content_type,
        )

        if not as_bytes:
            try:
                record.value = maybe_parse_body(record.value, str(record.content_type))
            except ValueError:
                logger.exception('Error parsing key-value store record')

        async with existing_store_by_id.file_operation_lock:
            await existing_store_by_id.update_timestamps(has_been_modified=False)

        return record

    async def get_record(self, key: str) -> KeyValueStoreRecord | None:
        """Retrieve the given record from the key-value store.

        Args:
            key: Key of the record to retrieve

        Returns:
            The requested record, or None, if the record does not exist
        """
        return await self._get_record_internal(key)

    async def get_record_as_bytes(self, key: str) -> KeyValueStoreRecord | None:
        """Retrieve the given record from the key-value store, without parsing it.

        Args:
            key: Key of the record to retrieve

        Returns:
            The requested record, or None, if the record does not exist
        """
        return await self._get_record_internal(key, as_bytes=True)

    async def stream_record(self, _key: str) -> AsyncIterator[dict | None]:
        """Stream the given record from the key-value store."""
        raise NotImplementedError('This method is not supported in local memory storage.')

    async def set_record(self, key: str, value: Any, content_type: str | None = None) -> None:
        """Set a value to the given record in the key-value store.

        Args:
            key: The key of the record to save the value to
            value: The value to save into the record
            content_type: The content type of the saved value
        """
        # Check by id
        existing_store_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id_=self.id,
            name=self.name,
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self.id)

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
            s = await json_dumps(value)
            value = s.encode('utf-8')

        async with existing_store_by_id.file_operation_lock:
            await existing_store_by_id.update_timestamps(has_been_modified=True)
            record = KeyValueStoreRecord(
                key=key,
                value=value,
                content_type=content_type,
            )

            old_record = existing_store_by_id.records.get(key)
            existing_store_by_id.records[key] = record

            if self._memory_storage_client.persist_storage:
                record_filename = self._filename_from_record(record)

                if old_record is not None and self._filename_from_record(old_record) != record_filename:
                    await existing_store_by_id.delete_persisted_record(old_record)

                await existing_store_by_id.persist_record(record)

    async def persist_record(self, record: KeyValueStoreRecord) -> None:
        """Persist the specified record to the key-value store."""
        store_directory = self.resource_directory
        record_filename = self._filename_from_record(record)
        record.filename = record_filename

        # Ensure the directory for the entity exists
        await makedirs(store_directory, exist_ok=True)

        # Create files for the record
        record_path = os.path.join(store_directory, record_filename)
        record_metadata_path = os.path.join(store_directory, record_filename + '.__metadata__.json')

        # Convert to bytes if string
        if isinstance(record.value, str):
            record.value = record.value.encode('utf-8')

        async with aiofiles.open(record_path, mode='wb') as f:
            await f.write(record.value)

        if self._memory_storage_client.write_metadata:
            async with aiofiles.open(record_metadata_path, mode='wb') as f:
                s = await json_dumps({'key': record.key, 'contentType': record.content_type})
                await f.write(s.encode('utf-8'))

    async def delete_record(self, key: str) -> None:
        """Delete the specified record from the key-value store.

        Args:
            key: The key of the record which to delete
        """
        # Check by id
        existing_store_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id_=self.id,
            name=self.name,
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self.id)

        record = existing_store_by_id.records.get(key)

        if record is not None:
            async with existing_store_by_id.file_operation_lock:
                del existing_store_by_id.records[key]
                await existing_store_by_id.update_timestamps(has_been_modified=True)
                if self._memory_storage_client.persist_storage:
                    await existing_store_by_id.delete_persisted_record(record)

    async def delete_persisted_record(self, record: KeyValueStoreRecord) -> None:
        """Delete the specified record from the key-value store."""
        store_directory = self.resource_directory
        record_filename = self._filename_from_record(record)

        # Ensure the directory for the entity exists
        await makedirs(store_directory, exist_ok=True)

        # Create files for the record
        record_path = os.path.join(store_directory, record_filename)
        record_metadata_path = os.path.join(store_directory, record_filename + '.__metadata__.json')

        await force_remove(record_path)
        await force_remove(record_metadata_path)

    async def update_timestamps(self, *, has_been_modified: bool) -> None:
        """Update the timestamps of the key-value store."""
        self._accessed_at = datetime.now(timezone.utc)

        if has_been_modified:
            self._modified_at = datetime.now(timezone.utc)

        kv_store_info = self.resource_info
        kv_store_info_as_dict = kv_store_info.__dict__

        await persist_metadata_if_enabled(
            data=kv_store_info_as_dict,
            entity_directory=self.resource_directory,
            write_metadata=self._memory_storage_client.write_metadata,
        )

    def _filename_from_record(self, record: KeyValueStoreRecord) -> str:
        if record.filename is not None:
            return record.filename

        if not record.content_type or record.content_type == 'application/octet-stream':
            return record.key

        extension = determine_file_extension(record.content_type)

        if record.key.endswith(f'.{extension}'):
            return record.key

        return f'{record.key}.{extension}'
