from __future__ import annotations

import asyncio
import json
import shutil
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import ValidationError
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from ._utils import METADATA_FILENAME, json_dumps

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

    _cache_by_name: ClassVar[dict[str, FileSystemKeyValueStoreClient]] = {}
    """A dictionary to cache clients by their names."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        storage_dir: Path,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemKeyValueStoreClient.open` class method to create a new instance.
        """
        self._metadata = KeyValueStoreMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
        )

        self._storage_dir = storage_dir

        # Internal attributes
        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

    @override
    @property
    def metadata(self) -> KeyValueStoreMetadata:
        return self._metadata

    @property
    def path_to_kvs(self) -> Path:
        """The full path to the key-value store directory."""
        return self._storage_dir / self._STORAGE_SUBDIR / self.metadata.name

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the key-value store metadata file."""
        return self.path_to_kvs / METADATA_FILENAME

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> FileSystemKeyValueStoreClient:
        if id:
            raise ValueError(
                'Opening a key-value store by "id" is not supported for file system storage client, use "name" instead.'
            )

        name = name or configuration.default_dataset_id

        # Check if the client is already cached by name.
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata(update_accessed_at=True)  # noqa: SLF001
            return client

        storage_dir = Path(configuration.storage_dir)
        kvs_path = storage_dir / cls._STORAGE_SUBDIR / name
        metadata_path = kvs_path / METADATA_FILENAME

        # If the key-value store directory exists, reconstruct the client from the metadata file.
        if kvs_path.exists():
            # If metadata file is missing, raise an error.
            if not metadata_path.exists():
                raise ValueError(f'Metadata file not found for key-value store "{name}"')

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
                id=metadata.id,
                name=name,
                created_at=metadata.created_at,
                accessed_at=metadata.accessed_at,
                modified_at=metadata.modified_at,
                storage_dir=storage_dir,
            )

            await client._update_metadata(update_accessed_at=True)

        # Otherwise, create a new key-value store client.
        else:
            now = datetime.now(timezone.utc)
            client = cls(
                id=crypto_random_object_id(),
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
                storage_dir=storage_dir,
            )
            await client._update_metadata()

        # Cache the client by name.
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        # If the client directory exists, remove it recursively.
        if self.path_to_kvs.exists():
            async with self._lock:
                await asyncio.to_thread(shutil.rmtree, self.path_to_kvs)

        # Remove the client from the cache.
        if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
            del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        # Update the metadata to record access
        await self._update_metadata(update_accessed_at=True)

        record_path = self.path_to_kvs / key

        if not record_path.exists():
            return None

        # Found a file for this key, now look for its metadata
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        if not record_metadata_filepath.exists():
            logger.warning(f'Found value file for key "{key}" but no metadata file.')
            return None

        # Read the metadata file
        async with self._lock:
            file = await asyncio.to_thread(open, record_metadata_filepath)
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
        value_bytes = await asyncio.to_thread(record_path.read_bytes)

        # Handle JSON values
        if 'application/json' in metadata.content_type:
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

        record_path = self.path_to_kvs / key

        # Prepare the metadata
        size = len(value_bytes)
        record_metadata = KeyValueStoreRecordMetadata(key=key, content_type=content_type, size=size)
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        record_metadata_content = await json_dumps(record_metadata.model_dump())

        async with self._lock:
            # Ensure the key-value store directory exists.
            await asyncio.to_thread(self.path_to_kvs.mkdir, parents=True, exist_ok=True)

            # Write the value to the file.
            await asyncio.to_thread(record_path.write_bytes, value_bytes)

            # Write the record metadata to the file.
            await asyncio.to_thread(
                record_metadata_filepath.write_text,
                record_metadata_content,
                encoding='utf-8',
            )

            # Update the KVS metadata to record the access and modification.
            await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def delete_value(self, *, key: str) -> None:
        record_path = self.path_to_kvs / key
        metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        deleted = False

        async with self._lock:
            # Delete the value file and its metadata if found
            if record_path.exists():
                await asyncio.to_thread(record_path.unlink)

                # Delete the metadata file if it exists
                if metadata_path.exists():
                    await asyncio.to_thread(metadata_path.unlink)
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

        count = 0
        async with self._lock:
            # Get all files in the KVS directory, sorted alphabetically
            files = sorted(await asyncio.to_thread(list, self.path_to_kvs.glob('*')))

            for file_path in files:
                # Skip the main metadata file
                if file_path.name == METADATA_FILENAME:
                    continue

                # Only process metadata files for records
                if not file_path.name.endswith(f'.{METADATA_FILENAME}'):
                    continue

                # Extract the base key name from the metadata filename
                key_name = file_path.name[: -len(f'.{METADATA_FILENAME}')]

                # Apply exclusive_start_key filter if provided
                if exclusive_start_key is not None and key_name <= exclusive_start_key:
                    continue

                # Try to read and parse the metadata file
                try:
                    metadata_content = await asyncio.to_thread(file_path.read_text, encoding='utf-8')
                    metadata_dict = json.loads(metadata_content)
                    record_metadata = KeyValueStoreRecordMetadata(**metadata_dict)

                    yield record_metadata

                    count += 1
                    if limit and count >= limit:
                        break

                except (json.JSONDecodeError, ValidationError) as e:
                    logger.warning(f'Failed to parse metadata file {file_path}: {e}')

        # Update accessed_at timestamp
        await self._update_metadata(update_accessed_at=True)

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for file system key-value stores.')

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
        await asyncio.to_thread(self.path_to_metadata.write_text, data, encoding='utf-8')
