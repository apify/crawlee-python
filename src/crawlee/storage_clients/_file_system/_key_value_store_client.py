from __future__ import annotations

import asyncio
import json
import shutil
import urllib.parse
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from typing_extensions import override

from crawlee._consts import METADATA_FILENAME
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import atomic_write, infer_mime_type, json_dumps
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee.configuration import Configuration


logger = getLogger(__name__)


class FileSystemKeyValueStoreClient(KeyValueStoreClient):
    """File system implementation of the key-value store client.

    This client persists data to the file system, making it suitable for scenarios where data needs to
    survive process restarts. Keys are mapped to file paths in a directory structure following the pattern:

    ```
    {STORAGE_DIR}/key_value_stores/{STORE_ID}/{KEY}
    ```

    Binary data is stored as-is, while JSON and text data are stored in human-readable format.
    The implementation automatically handles serialization based on the content type and
    maintains metadata about each record.

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
        storage_dir: Path,
        lock: asyncio.Lock,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemKeyValueStoreClient.open` class method to create a new instance.
        """
        self._metadata = metadata

        self._storage_dir = storage_dir
        """The base directory where the storage data are being persisted."""

        self._lock = lock
        """A lock to ensure that only one operation is performed at a time."""

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return self._metadata

    @property
    def path_to_kvs(self) -> Path:
        """The full path to the key-value store directory."""
        if self._metadata.name is None:
            return self._storage_dir / self._STORAGE_SUBDIR / self._STORAGE_SUBSUBDIR_DEFAULT

        return self._storage_dir / self._STORAGE_SUBDIR / self._metadata.name

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
        configuration: Configuration,
    ) -> FileSystemKeyValueStoreClient:
        """Open or create a file system key-value store client.

        This method attempts to open an existing key-value store from the file system. If a KVS with the specified
        ID or name exists, it loads the metadata from the stored files. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store to open. If provided, searches for existing store by ID.
            name: The name of the key-value store to open. If not provided, uses the default store.
            configuration: The configuration object containing storage directory settings.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a store with the specified ID is not found, or if metadata is invalid.
        """
        storage_dir = Path(configuration.storage_dir)
        kvs_base_path = storage_dir / cls._STORAGE_SUBDIR

        if not kvs_base_path.exists():
            await asyncio.to_thread(kvs_base_path.mkdir, parents=True, exist_ok=True)

        # Get a new instance by ID.
        if id:
            found = False
            for kvs_dir in kvs_base_path.iterdir():
                if not kvs_dir.is_dir():
                    continue

                metadata_path = kvs_dir / METADATA_FILENAME
                if not metadata_path.exists():
                    continue

                try:
                    file = await asyncio.to_thread(metadata_path.open)
                    try:
                        file_content = json.load(file)
                        metadata = KeyValueStoreMetadata(**file_content)
                        if metadata.id == id:
                            client = cls(
                                metadata=metadata,
                                storage_dir=storage_dir,
                                lock=asyncio.Lock(),
                            )
                            await client._update_metadata(update_accessed_at=True)
                            found = True
                            break
                    finally:
                        await asyncio.to_thread(file.close)
                except (json.JSONDecodeError, ValidationError):
                    continue

            if not found:
                raise ValueError(f'Key-value store with ID "{id}" not found.')

        # Get a new instance by name.
        else:
            kvs_path = kvs_base_path / cls._STORAGE_SUBSUBDIR_DEFAULT if name is None else kvs_base_path / name
            metadata_path = kvs_path / METADATA_FILENAME

            # If the key-value store directory exists, reconstruct the client from the metadata file.
            if kvs_path.exists() and metadata_path.exists():
                file = await asyncio.to_thread(open, metadata_path)
                try:
                    file_content = json.load(file)
                finally:
                    await asyncio.to_thread(file.close)
                try:
                    metadata = KeyValueStoreMetadata(**file_content)
                except ValidationError as exc:
                    raise ValueError(f'Invalid metadata file for key-value store "{name}"') from exc

                client = cls(
                    metadata=metadata,
                    storage_dir=storage_dir,
                    lock=asyncio.Lock(),
                )

                await client._update_metadata(update_accessed_at=True)

            # Otherwise, create a new key-value store client.
            else:
                now = datetime.now(timezone.utc)
                metadata = KeyValueStoreMetadata(
                    id=crypto_random_object_id(),
                    name=name,
                    created_at=now,
                    accessed_at=now,
                    modified_at=now,
                )
                client = cls(
                    metadata=metadata,
                    storage_dir=storage_dir,
                    lock=asyncio.Lock(),
                )
                await client._update_metadata()

        return client

    @override
    async def drop(self) -> None:
        # If the client directory exists, remove it recursively.
        if self.path_to_kvs.exists():
            async with self._lock:
                await asyncio.to_thread(shutil.rmtree, self.path_to_kvs)

    @override
    async def purge(self) -> None:
        async with self._lock:
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

        record_path = self.path_to_kvs / self._encode_key(key)

        if not record_path.exists():
            return None

        # Found a file for this key, now look for its metadata
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        if not record_metadata_filepath.exists():
            logger.warning(f'Found value file for key "{key}" but no metadata file.')
            return None

        # Read the metadata file
        async with self._lock:
            try:
                file = await asyncio.to_thread(open, record_metadata_filepath)
            except FileNotFoundError:
                logger.warning(f'Metadata file disappeared for key "{key}", aborting get_value')
                return None

            try:
                metadata_content = json.load(file)
            except json.JSONDecodeError:
                logger.warning(f'Invalid metadata file for key "{key}"')
                return None
            finally:
                await asyncio.to_thread(file.close)

        try:
            metadata = KeyValueStoreRecordMetadata(**metadata_content)
        except ValidationError:
            logger.warning(f'Invalid metadata schema for key "{key}"')
            return None

        # Read the actual value
        try:
            value_bytes = await asyncio.to_thread(record_path.read_bytes)
        except FileNotFoundError:
            logger.warning(f'Value file disappeared for key "{key}"')
            return None

        # Handle None values
        if metadata.content_type == 'application/x-none':
            value = None
        # Handle JSON values
        elif 'application/json' in metadata.content_type:
            try:
                value = json.loads(value_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(f'Failed to decode JSON value for key "{key}"')
                return None
        # Handle text values
        elif metadata.content_type.startswith('text/'):
            try:
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f'Failed to decode text value for key "{key}"')
                return None
        # Handle binary values
        else:
            value = value_bytes

        # Calculate the size of the value in bytes
        size = len(value_bytes)

        return KeyValueStoreRecord(
            key=metadata.key,
            value=value,
            content_type=metadata.content_type,
            size=size,
        )

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        # Special handling for None values
        if value is None:
            content_type = 'application/x-none'  # Special content type to identify None values
            value_bytes = b''
        else:
            content_type = content_type or infer_mime_type(value)

            # Serialize the value to bytes.
            if 'application/json' in content_type:
                value_bytes = (await json_dumps(value)).encode('utf-8')
            elif isinstance(value, str):
                value_bytes = value.encode('utf-8')
            elif isinstance(value, (bytes, bytearray)):
                value_bytes = value
            else:
                # Fallback: attempt to convert to string and encode.
                value_bytes = str(value).encode('utf-8')

        record_path = self.path_to_kvs / self._encode_key(key)

        # Prepare the metadata
        size = len(value_bytes)
        record_metadata = KeyValueStoreRecordMetadata(key=key, content_type=content_type, size=size)
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        record_metadata_content = await json_dumps(record_metadata.model_dump())

        async with self._lock:
            # Ensure the key-value store directory exists.
            await asyncio.to_thread(self.path_to_kvs.mkdir, parents=True, exist_ok=True)

            # Write the value to the file.
            await atomic_write(record_path, value_bytes)

            # Write the record metadata to the file.
            await atomic_write(record_metadata_filepath, record_metadata_content)

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
                await asyncio.to_thread(record_path.unlink, missing_ok=True)

                # Delete the metadata file if it exists
                if metadata_path.exists():
                    await asyncio.to_thread(metadata_path.unlink, missing_ok=True)
                else:
                    logger.warning(f'Found value file for key "{key}" but no metadata file when trying to delete it.')

                deleted = True

            # If we deleted something, update the KVS metadata
            if deleted:
                await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        # Check if the KVS directory exists
        if not self.path_to_kvs.exists():
            return

        # List and sort all files *inside* a brief lock, then release it immediately:
        async with self._lock:
            files = sorted(await asyncio.to_thread(list, self.path_to_kvs.glob('*')))

        count = 0

        for file_path in files:
            # Skip the main metadata file
            if file_path.name == METADATA_FILENAME:
                continue

            # Only process metadata files for records
            if not file_path.name.endswith(f'.{METADATA_FILENAME}'):
                continue

            # Extract the base key name from the metadata filename
            key_name = self._decode_key(file_path.name[: -len(f'.{METADATA_FILENAME}')])

            # Apply exclusive_start_key filter if provided
            if exclusive_start_key is not None and key_name <= exclusive_start_key:
                continue

            # Try to read and parse the metadata file
            try:
                metadata_content = await asyncio.to_thread(file_path.read_text, encoding='utf-8')
            except FileNotFoundError:
                logger.warning(f'Metadata file disappeared for key "{key_name}", skipping it.')
                continue

            try:
                metadata_dict = json.loads(metadata_content)
            except json.JSONDecodeError:
                logger.warning(f'Failed to decode metadata file for key "{key_name}", skipping it.')
                continue

            try:
                record_metadata = KeyValueStoreRecordMetadata(**metadata_dict)
            except ValidationError:
                logger.warning(f'Invalid metadata schema for key "{key_name}", skipping it.')

            yield record_metadata

            count += 1
            if limit and count >= limit:
                break

        # Update accessed_at timestamp
        async with self._lock:
            await self._update_metadata(update_accessed_at=True)

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
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now

        # Ensure the parent directory for the metadata file exists.
        await asyncio.to_thread(self.path_to_metadata.parent.mkdir, parents=True, exist_ok=True)

        # Dump the serialized metadata to the file.
        data = await json_dumps(self._metadata.model_dump())
        await atomic_write(self.path_to_metadata, data)

    def _encode_key(self, key: str) -> str:
        """Encode a key to make it safe for use in a file path."""
        return urllib.parse.quote(key, safe='')

    def _decode_key(self, encoded_key: str) -> str:
        """Decode a key that was encoded to make it safe for use in a file path."""
        return urllib.parse.unquote(encoded_key)
