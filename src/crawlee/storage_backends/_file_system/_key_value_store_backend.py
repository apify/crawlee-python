from __future__ import annotations

import asyncio
import json
import os
import shutil
import urllib.parse
from datetime import UTC, datetime
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Self

from pydantic import ValidationError
from typing_extensions import override

from crawlee._consts import METADATA_FILENAME
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import atomic_write, json_dumps, validate_subdirectory
from crawlee._utils.raise_if_too_many_kwargs import raise_if_too_many_kwargs
from crawlee.storage_backends._base import KeyValueStoreBackend
from crawlee.storage_backends._utils import SortedKeys
from crawlee.storage_backends.models import (
    KeyValueStoreKeysListPage,
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from crawlee.configuration import Configuration


logger = getLogger(__name__)


class FileSystemKeyValueStoreBackend(KeyValueStoreBackend):
    """File system implementation of the key-value store backend.

    This backend persists data to the file system, making it suitable for scenarios where data needs to
    survive process restarts. Keys are mapped to file paths in a directory structure following the pattern:

    ```
    {STORAGE_DIR}/key_value_stores/{STORE_ID}/{KEY}
    ```

    The values are stored as the bytes `KeyValueStore` serializes them to, so JSON and text data stay human-readable.
    The content type and the size of each record are kept in a metadata file next to it.

    This implementation is ideal for long-running crawlers where persistence is important and
    for development environments where you want to easily inspect the stored data between runs.
    """

    _STORAGE_SUBDIR = 'key_value_stores'
    """The name of the subdirectory where key-value stores are stored."""

    _STORAGE_SUBSUBDIR_DEFAULT = 'default'
    """The name of the subdirectory for the default key-value store."""

    def __init__(
        self,
        *,
        metadata: KeyValueStoreMetadata,
        path_to_kvs: Path,
        lock: asyncio.Lock,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemKeyValueStoreBackend.open` class method to create a new instance.
        """
        self._metadata = metadata

        self._path_to_kvs = path_to_kvs
        """The full path to the key-value store directory."""

        self._lock = lock
        """A lock to ensure that only one operation is performed at a time."""

        self._sorted_keys: SortedKeys | None = None
        """The record keys, read from the directory when a listing starts and kept up to date by own writes. Dropped
        after the last page, on a purge or drop, and when an own write fails or is cancelled, so that the next listing
        reads it again.
        """

        self._next_exclusive_start_key: str | None = None
        """The `exclusive_start_key` of the next page after the last listed page."""

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return self._metadata

    @property
    def path_to_kvs(self) -> Path:
        """The full path to the key-value store directory."""
        return self._path_to_kvs

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the key-value store metadata file."""
        return self.path_to_kvs / METADATA_FILENAME

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        configuration: Configuration,
    ) -> Self:
        """Open or create a file system key-value store backend.

        This method attempts to open an existing key-value store from the file system. If a KVS with the specified
        ID or name exists, it loads the metadata from the stored files. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store to open. If provided, searches for existing store by ID.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.
            configuration: The configuration object containing storage directory settings.

        Returns:
            An instance for the opened or created storage backend.

        Raises:
            ValueError: If a store with the specified ID is not found, if metadata is invalid,
                or if both name and alias are provided.
        """
        # Validate input parameters.
        raise_if_too_many_kwargs(id=id, name=name, alias=alias)

        kvs_base_path = Path(configuration.storage_dir) / cls._STORAGE_SUBDIR

        if not kvs_base_path.exists():
            await asyncio.to_thread(kvs_base_path.mkdir, parents=True, exist_ok=True)

        # Get a new instance by ID.
        if id:
            found = False
            for kvs_dir in kvs_base_path.iterdir():
                if not kvs_dir.is_dir():
                    continue

                path_to_metadata = kvs_dir / METADATA_FILENAME
                if not path_to_metadata.exists():
                    continue

                try:
                    file = await asyncio.to_thread(path_to_metadata.open, mode='r', encoding='utf-8')
                    try:
                        file_content = json.load(file)
                        metadata = KeyValueStoreMetadata(**file_content)
                        if metadata.id == id:
                            backend = cls(
                                metadata=metadata,
                                path_to_kvs=kvs_base_path / kvs_dir,
                                lock=asyncio.Lock(),
                            )
                            await backend._update_metadata(update_accessed_at=True)
                            found = True
                            break
                    finally:
                        await asyncio.to_thread(file.close)
                except (json.JSONDecodeError, ValidationError):
                    continue

            if not found:
                raise ValueError(f'Key-value store with ID "{id}" not found.')

        # Get a new instance by name or alias.
        else:
            path_to_kvs = validate_subdirectory(kvs_base_path, name or alias or 'default')
            path_to_metadata = path_to_kvs / METADATA_FILENAME

            # If the key-value store directory exists, reconstruct the backend from the metadata file.
            if path_to_kvs.exists() and path_to_metadata.exists():
                file = await asyncio.to_thread(path_to_metadata.open, mode='r', encoding='utf-8')
                try:
                    file_content = json.load(file)
                finally:
                    await asyncio.to_thread(file.close)
                try:
                    metadata = KeyValueStoreMetadata(**file_content)
                except ValidationError as exc:
                    raise ValueError(f'Invalid metadata file for key-value store "{name or alias}"') from exc

                backend = cls(
                    metadata=metadata,
                    path_to_kvs=path_to_kvs,
                    lock=asyncio.Lock(),
                )

                await backend._update_metadata(update_accessed_at=True)

            # Otherwise, create a new key-value store backend.
            else:
                now = datetime.now(UTC)
                metadata = KeyValueStoreMetadata(
                    id=crypto_random_object_id(),
                    name=name,
                    created_at=now,
                    accessed_at=now,
                    modified_at=now,
                )
                backend = cls(
                    metadata=metadata,
                    path_to_kvs=path_to_kvs,
                    lock=asyncio.Lock(),
                )
                await backend._update_metadata()

        return backend

    @override
    async def drop(self) -> None:
        # If the storage directory exists, remove it recursively.
        if self.path_to_kvs.exists():
            async with self._lock:
                self._sorted_keys = None
                await asyncio.to_thread(shutil.rmtree, self.path_to_kvs)

    @override
    async def purge(self) -> None:
        async with self._lock:
            # Dropped before the files, so that a purge that fails or is cancelled midway leaves no stale keys.
            self._sorted_keys = None

            for file_path in self.path_to_kvs.glob('*'):
                if file_path.name == METADATA_FILENAME:
                    continue
                await asyncio.to_thread(file_path.unlink, missing_ok=True)

            await self._update_metadata(
                update_accessed_at=True,
                update_modified_at=True,
            )

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        # Update the metadata to record access
        async with self._lock:
            await self._update_metadata(update_accessed_at=True)

            # Read under the lock, so that the metadata and the value come from the same write.
            return await asyncio.to_thread(self._read_record, key)

    @override
    async def get_values(self, *, keys: Sequence[str]) -> Mapping[str, KeyValueStoreRecord]:
        # Update the metadata to record access
        async with self._lock:
            await self._update_metadata(update_accessed_at=True)

            # Read under the lock, so that the metadata and the value of each record come from the same write.
            return await asyncio.to_thread(self._read_records, keys)

    @override
    async def set_value(self, *, key: str, value: bytes, content_type: str) -> None:
        record_path = self.path_to_kvs / self._encode_key(key)

        # Prepare the metadata
        record_metadata = KeyValueStoreRecordMetadata(key=key, content_type=content_type, size=len(value))
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        record_metadata_content = await json_dumps(record_metadata.model_dump(by_alias=True))

        async with self._lock:
            # Ensure the key-value store directory exists.
            await asyncio.to_thread(self.path_to_kvs.mkdir, parents=True, exist_ok=True)

            try:
                # Write the value to the file.
                await atomic_write(record_path, value)

                # Write the record metadata to the file.
                await atomic_write(record_metadata_filepath, record_metadata_content)
            except BaseException:
                # A write that fails or is cancelled may still change the directory, so the next listing reads it again.
                self._sorted_keys = None
                raise

            if self._sorted_keys is not None:
                self._sorted_keys.add(key)

            # Update the KVS metadata to record the access and modification.
            await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def delete_value(self, *, key: str) -> None:
        record_path = self.path_to_kvs / self._encode_key(key)
        metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        deleted = False

        async with self._lock:
            # Delete the value file and its metadata if found
            if record_path.exists():
                try:
                    await asyncio.to_thread(record_path.unlink, missing_ok=True)

                    # Delete the metadata file if it exists
                    if metadata_path.exists():
                        await asyncio.to_thread(metadata_path.unlink, missing_ok=True)
                    else:
                        logger.warning(
                            'Found value file for key "%s" but no metadata file when trying to delete it.', key
                        )
                except BaseException:
                    # A delete that fails or is cancelled may still change the directory, so the next listing reads
                    # it again.
                    self._sorted_keys = None
                    raise

                deleted = True

            # If we deleted something, update the KVS metadata
            if deleted:
                if self._sorted_keys is not None:
                    self._sorted_keys.discard(key)
                await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def list_keys(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> KeyValueStoreKeysListPage:
        async with self._lock:
            # Check if the KVS directory exists
            if not self.path_to_kvs.exists():
                self._sorted_keys = None
                return KeyValueStoreKeysListPage(
                    items=[], count=0, limit=limit, exclusive_start_key=exclusive_start_key
                )

            # A listing that doesn't continue from the last listed page starts from the current directory content.
            if (
                self._sorted_keys is None
                or exclusive_start_key is None
                or exclusive_start_key != self._next_exclusive_start_key
            ):
                self._sorted_keys = await asyncio.to_thread(self._read_sorted_keys)

            keys, next_exclusive_start_key = self._sorted_keys.page(
                prefix=prefix,
                exclusive_start_key=exclusive_start_key,
                limit=limit,
            )
            self._next_exclusive_start_key = next_exclusive_start_key

            # No listing continues after the last page, so the next one reads the directory again anyway.
            if next_exclusive_start_key is None:
                self._sorted_keys = None

        items = await asyncio.to_thread(self._read_records_metadata, keys)

        # Update accessed_at timestamp
        async with self._lock:
            await self._update_metadata(update_accessed_at=True)

        return KeyValueStoreKeysListPage(
            items=items,
            count=len(items),
            limit=limit,
            exclusive_start_key=exclusive_start_key,
            next_exclusive_start_key=next_exclusive_start_key,
        )

    @override
    async def get_public_url(self, *, key: str) -> str:
        """Return a file:// URL for the given key.

        Args:
            key: The key to get the public URL for.

        Returns:
            A file:// URL pointing to the file on the local filesystem.
        """
        record_path = self.path_to_kvs / self._encode_key(key)
        absolute_path = record_path.absolute()
        return absolute_path.as_uri()

    @override
    async def record_exists(self, *, key: str) -> bool:
        """Check if a record with the given key exists in the key-value store.

        Args:
            key: The key to check for existence.

        Returns:
            True if a record with the given key exists, False otherwise.
        """
        # Update the metadata to record access
        async with self._lock:
            await self._update_metadata(update_accessed_at=True)

        record_path = self.path_to_kvs / self._encode_key(key)
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')

        # Both the value file and metadata file must exist for a record to be considered existing
        return record_path.exists() and record_metadata_filepath.exists()

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the KVS metadata file with current information.

        Args:
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(UTC)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now

        # Ensure the parent directory for the metadata file exists.
        await asyncio.to_thread(self.path_to_metadata.parent.mkdir, parents=True, exist_ok=True)

        # Dump the serialized metadata to the file.
        data = await json_dumps(self._metadata.model_dump(by_alias=True))
        await atomic_write(self.path_to_metadata, data)

    def _read_record(self, key: str) -> KeyValueStoreRecord | None:
        """Read the record of the key from its files, or return `None` when it has none or they can't be read."""
        record_path = self.path_to_kvs / self._encode_key(key)

        if not record_path.exists():
            return None

        # Found a file for this key, now look for its metadata
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        if not record_metadata_filepath.exists():
            logger.warning('Found value file for key "%s" but no metadata file.', key)
            return None

        # Read the metadata file
        try:
            metadata_content = json.loads(record_metadata_filepath.read_text(encoding='utf-8'))
        except FileNotFoundError:
            logger.warning('Metadata file disappeared for key "%s", aborting get_value', key)
            return None
        except json.JSONDecodeError:
            logger.warning('Invalid metadata file for key "%s"', key)
            return None

        try:
            metadata = KeyValueStoreRecordMetadata(**metadata_content)
        except ValidationError:
            logger.warning('Invalid metadata schema for key "%s"', key)
            return None

        # Read the actual value
        try:
            value_bytes = record_path.read_bytes()
        except FileNotFoundError:
            logger.warning('Value file disappeared for key "%s"', key)
            return None

        return KeyValueStoreRecord(
            key=metadata.key,
            value=value_bytes,
            content_type=metadata.content_type,
            size=len(value_bytes),
        )

    def _read_records(self, keys: Sequence[str]) -> dict[str, KeyValueStoreRecord]:
        """Read the records of the keys that have one."""
        records = dict[str, KeyValueStoreRecord]()
        for key in keys:
            record = self._read_record(key)
            if record is not None:
                records[key] = record
        return records

    def _read_records_metadata(self, keys: Sequence[str]) -> list[KeyValueStoreRecordMetadata]:
        """Read the metadata of the records of the keys, skipping the ones whose metadata can't be read."""
        items = list[KeyValueStoreRecordMetadata]()

        for key in keys:
            file_path = self.path_to_kvs / f'{self._encode_key(key)}.{METADATA_FILENAME}'

            # Try to read and parse the metadata file
            try:
                metadata_content = file_path.read_text(encoding='utf-8')
            except FileNotFoundError:
                logger.warning('Metadata file disappeared for key "%s", skipping it.', key)
                continue

            try:
                metadata_dict = json.loads(metadata_content)
            except json.JSONDecodeError:
                logger.warning('Failed to decode metadata file for key "%s", skipping it.', key)
                continue

            try:
                record_metadata = KeyValueStoreRecordMetadata(**metadata_dict)
            except ValidationError:
                logger.warning('Invalid metadata schema for key "%s", skipping it.', key)
                continue

            items.append(record_metadata)

        return items

    def _read_sorted_keys(self) -> SortedKeys:
        """Read the record keys from the names of their metadata files."""
        metadata_suffix = f'.{METADATA_FILENAME}'
        keys = list[str]()

        with os.scandir(self.path_to_kvs) as entries:
            for entry in entries:
                if entry.name.endswith(metadata_suffix):
                    keys.append(self._decode_key(entry.name.removesuffix(metadata_suffix)))

        return SortedKeys(keys)

    def _encode_key(self, key: str) -> str:
        """Encode a key to make it safe for use in a file path."""
        return urllib.parse.quote(key, safe='')

    def _decode_key(self, encoded_key: str) -> str:
        """Decode a key that was encoded to make it safe for use in a file path."""
        return urllib.parse.unquote(encoded_key)
