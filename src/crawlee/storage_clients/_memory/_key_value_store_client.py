from __future__ import annotations

import asyncio
import io
import os
import shutil
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._types import StorageTypes
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.data_processing import maybe_parse_body, raise_on_duplicate_storage, raise_on_non_existing_storage
from crawlee._utils.file import determine_file_extension, force_remove, force_rename, is_file_or_bytes, json_dumps
from crawlee.storage_clients._base import KeyValueStoreClient as BaseKeyValueStoreClient
from crawlee.storage_clients.models import (
    KeyValueStoreKeyInfo,
    KeyValueStoreListKeysPage,
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
)

from ._creation_management import find_or_create_client_by_id_or_name_inner, persist_metadata_if_enabled

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from httpx import Response

    from crawlee.storage_clients import MemoryStorageClient

logger = getLogger(__name__)


class KeyValueStoreClient(BaseKeyValueStoreClient):
    """Subclient for manipulating a single key-value store."""

    def __init__(
        self,
        *,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,
        name: str | None = None,
        created_at: datetime | None = None,
        accessed_at: datetime | None = None,
        modified_at: datetime | None = None,
    ) -> None:
        self.id = id or crypto_random_object_id()
        self.name = name

        self._memory_storage_client = memory_storage_client
        self._created_at = created_at or datetime.now(timezone.utc)
        self._accessed_at = accessed_at or datetime.now(timezone.utc)
        self._modified_at = modified_at or datetime.now(timezone.utc)

        self.records: dict[str, KeyValueStoreRecord] = {}
        self.file_operation_lock = asyncio.Lock()

    @property
    def resource_info(self) -> KeyValueStoreMetadata:
        """Get the resource info for the key-value store client."""
        return KeyValueStoreMetadata(
            id=self.id,
            name=self.name,
            accessed_at=self._accessed_at,
            created_at=self._created_at,
            modified_at=self._modified_at,
            user_id='1',
        )

    @property
    def resource_directory(self) -> str:
        """Get the resource directory for the client."""
        return os.path.join(self._memory_storage_client.key_value_stores_directory, self.name or self.id)

    @override
    async def get(self) -> KeyValueStoreMetadata | None:
        found = find_or_create_client_by_id_or_name_inner(
            resource_client_class=KeyValueStoreClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if found:
            async with found.file_operation_lock:
                await found.update_timestamps(has_been_modified=False)
                return found.resource_info

        return None

    @override
    async def update(self, *, name: str | None = None) -> KeyValueStoreMetadata:
        # Check by id
        existing_store_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=KeyValueStoreClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
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

            previous_dir = existing_store_by_id.resource_directory
            existing_store_by_id.name = name

            await force_rename(previous_dir, existing_store_by_id.resource_directory)

            # Update timestamps
            await existing_store_by_id.update_timestamps(has_been_modified=True)

        return existing_store_by_id.resource_info

    @override
    async def delete(self) -> None:
        store = next(
            (store for store in self._memory_storage_client.key_value_stores_handled if store.id == self.id), None
        )

        if store is not None:
            async with store.file_operation_lock:
                self._memory_storage_client.key_value_stores_handled.remove(store)
                store.records.clear()

                if os.path.exists(store.resource_directory):
                    await asyncio.to_thread(shutil.rmtree, store.resource_directory)

    @override
    async def list_keys(
        self,
        *,
        limit: int = 1000,
        exclusive_start_key: str | None = None,
    ) -> KeyValueStoreListKeysPage:
        # Check by id
        existing_store_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=KeyValueStoreClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self.id)

        items: list[KeyValueStoreKeyInfo] = []

        for record in existing_store_by_id.records.values():
            size = len(record.value)
            items.append(KeyValueStoreKeyInfo(key=record.key, size=size))

        if len(items) == 0:
            return KeyValueStoreListKeysPage(
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

        return KeyValueStoreListKeysPage(
            count=len(items),
            limit=limit,
            exclusive_start_key=exclusive_start_key,
            is_truncated=not is_last_selected_item_absolutely_last,
            next_exclusive_start_key=next_exclusive_start_key,
            items=limited_items,
        )

    @override
    async def get_record(self, key: str) -> KeyValueStoreRecord | None:
        return await self._get_record_internal(key)

    @override
    async def get_record_as_bytes(self, key: str) -> KeyValueStoreRecord[bytes] | None:
        return await self._get_record_internal(key, as_bytes=True)

    @override
    async def stream_record(self, key: str) -> AbstractAsyncContextManager[KeyValueStoreRecord[Response] | None]:
        raise NotImplementedError('This method is not supported in memory storage.')

    @override
    async def set_record(self, key: str, value: Any, content_type: str | None = None) -> None:
        # Check by id
        existing_store_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=KeyValueStoreClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
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
            record = KeyValueStoreRecord(key=key, value=value, content_type=content_type, filename=None)

            old_record = existing_store_by_id.records.get(key)
            existing_store_by_id.records[key] = record

            if self._memory_storage_client.persist_storage:
                record_filename = self._filename_from_record(record)
                record.filename = record_filename

                if old_record is not None and self._filename_from_record(old_record) != record_filename:
                    await existing_store_by_id.delete_persisted_record(old_record)

                await existing_store_by_id.persist_record(record)

    @override
    async def delete_record(self, key: str) -> None:
        # Check by id
        existing_store_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=KeyValueStoreClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
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

    @override
    async def get_public_url(self, key: str) -> str:
        existing_store_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=KeyValueStoreClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_store_by_id is None:
            raise_on_non_existing_storage(StorageTypes.KEY_VALUE_STORE, self.id)

        record = await self._get_record_internal(key)

        if not record:
            raise ValueError(f'Record with key "{key}" was not found.')

        resource_dir = existing_store_by_id.resource_directory
        record_filename = self._filename_from_record(record)
        record_path = os.path.join(resource_dir, record_filename)
        return f'file://{record_path}'

    async def persist_record(self, record: KeyValueStoreRecord) -> None:
        """Persist the specified record to the key-value store."""
        store_directory = self.resource_directory
        record_filename = self._filename_from_record(record)
        record.filename = record_filename
        record.content_type = record.content_type or 'application/octet-stream'

        # Ensure the directory for the entity exists
        await asyncio.to_thread(os.makedirs, store_directory, exist_ok=True)

        # Create files for the record
        record_path = os.path.join(store_directory, record_filename)
        record_metadata_path = os.path.join(store_directory, f'{record_filename}.__metadata__.json')

        # Convert to bytes if string
        if isinstance(record.value, str):
            record.value = record.value.encode('utf-8')

        f = await asyncio.to_thread(open, record_path, mode='wb')
        try:
            await asyncio.to_thread(f.write, record.value)
        finally:
            await asyncio.to_thread(f.close)

        if self._memory_storage_client.write_metadata:
            metadata_f = await asyncio.to_thread(open, record_metadata_path, mode='wb')

            try:
                record_metadata = KeyValueStoreRecordMetadata(key=record.key, content_type=record.content_type)
                await asyncio.to_thread(metadata_f.write, record_metadata.model_dump_json(indent=2).encode('utf-8'))
            finally:
                await asyncio.to_thread(metadata_f.close)

    async def delete_persisted_record(self, record: KeyValueStoreRecord) -> None:
        """Delete the specified record from the key-value store."""
        store_directory = self.resource_directory
        record_filename = self._filename_from_record(record)

        # Ensure the directory for the entity exists
        await asyncio.to_thread(os.makedirs, store_directory, exist_ok=True)

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

        await persist_metadata_if_enabled(
            data=self.resource_info.model_dump(),
            entity_directory=self.resource_directory,
            write_metadata=self._memory_storage_client.write_metadata,
        )

    async def _get_record_internal(
        self,
        key: str,
        *,
        as_bytes: bool = False,
    ) -> KeyValueStoreRecord | None:
        # Check by id
        existing_store_by_id = find_or_create_client_by_id_or_name_inner(
            resource_client_class=KeyValueStoreClient,
            memory_storage_client=self._memory_storage_client,
            id=self.id,
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
            filename=stored_record.filename,
        )

        if not as_bytes:
            try:
                record.value = maybe_parse_body(record.value, str(record.content_type))
            except ValueError:
                logger.exception('Error parsing key-value store record')

        async with existing_store_by_id.file_operation_lock:
            await existing_store_by_id.update_timestamps(has_been_modified=False)

        return record

    def _filename_from_record(self, record: KeyValueStoreRecord) -> str:
        if record.filename is not None:
            return record.filename

        if not record.content_type or record.content_type == 'application/octet-stream':
            return record.key

        extension = determine_file_extension(record.content_type)

        if record.key.endswith(f'.{extension}'):
            return record.key

        return f'{record.key}.{extension}'
