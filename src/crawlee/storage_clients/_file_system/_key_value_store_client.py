from __future__ import annotations

import asyncio
import json
import mimetypes
import shutil
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreListKeysPage, KeyValueStoreMetadata, KeyValueStoreRecord

from ._utils import METADATA_FILENAME, json_dumps

if TYPE_CHECKING:
    from pathlib import Path

logger = getLogger(__name__)


class FileSystemKeyValueStoreClient(KeyValueStoreClient):
    """A file system key-value store (KVS) implementation."""

    _DEFAULT_NAME = 'default'
    """The name of the unnamed KVS."""

    _STORAGE_SUBDIR = 'key_value_store'
    """The name of the subdirectory where KVSs are stored."""

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
        self._id = id
        self._name = name
        self._created_at = created_at
        self._accessed_at = accessed_at
        self._modified_at = modified_at
        self._storage_dir = storage_dir

        # Internal attributes.
        self._lock = asyncio.Lock()
        """A lock to ensure that only one file operation is performed at a time."""

    @override
    @property
    def id(self) -> str:
        return self._id

    @override
    @property
    def name(self) -> str | None:
        return self._name

    @override
    @property
    def created_at(self) -> datetime:
        return self._created_at

    @override
    @property
    def accessed_at(self) -> datetime:
        return self._accessed_at

    @override
    @property
    def modified_at(self) -> datetime:
        return self._modified_at

    @property
    def _path_to_kvs(self) -> Path:
        """The full path to the key-value store directory."""
        return self._storage_dir / self._STORAGE_SUBDIR / self._name

    @property
    def _path_to_metadata(self) -> Path:
        """The full path to the key-value store metadata file."""
        return self._path_to_kvs / METADATA_FILENAME

    @override
    @classmethod
    async def open(
        cls,
        id: str | None,
        name: str | None,
        storage_dir: Path,
    ) -> FileSystemKeyValueStoreClient:
        """Open an existing key-value store client or create a new one if it does not exist.

        If the key-value store directory exists, this method reconstructs the client from the metadata file.
        Otherwise, a new key-value store client is created with a new unique ID.

        Args:
            id: The key-value store ID.
            name: The key-value store name; if not provided, defaults to the default name.
            storage_dir: The base directory for storage.

        Returns:
           A new instance of the file system key-value store client.
        """
        name = name or cls._DEFAULT_NAME
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
            client = cls(
                id=crypto_random_object_id(),
                name=name,
                created_at=datetime.now(timezone.utc),
                accessed_at=datetime.now(timezone.utc),
                modified_at=datetime.now(timezone.utc),
                storage_dir=storage_dir,
            )
            await client._update_metadata()

        return client

    @override
    async def drop(self) -> None:
        # If the key-value store directory exists, remove it recursively.
        if self._path_to_kvs.exists():
            async with self._lock:
                await asyncio.to_thread(shutil.rmtree, self._path_to_kvs)

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        pass

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        content_type = content_type or self._infer_mime_type(value)

        if 'application/json' in content_type:
            value = (await json_dumps(value)).encode('utf-8')
        elif isinstance(value, str):
            value = value.encode('utf-8')

        filename = self._infer_filename(key, content_type)

        async with self._lock:
            # Ensure the KVS directory exists.
            await asyncio.to_thread(self._path_to_kvs.mkdir, parents=True, exist_ok=True)
            record_path = self._path_to_kvs / filename

            # TODO

            await self._update_metadata(update_accessed_at=True, update_modified_at=True)

        # TODO

    @override
    async def delete_value(self, *, key: str) -> None:
        pass

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> KeyValueStoreListKeysPage:
        pass

    @override
    async def get_public_url(self, *, key: str) -> str:
        pass

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
        metadata = KeyValueStoreMetadata(
            id=self._id,
            name=self._name,
            created_at=self._created_at,
            accessed_at=now if update_accessed_at else self._accessed_at,
            modified_at=now if update_modified_at else self._modified_at,
        )

        # Ensure the parent directory for the metadata file exists.
        await asyncio.to_thread(self._path_to_metadata.parent.mkdir, parents=True, exist_ok=True)

        # Dump the serialized metadata to the file.
        data = await json_dumps(metadata.model_dump())
        await asyncio.to_thread(self._path_to_metadata.write_text, data, encoding='utf-8')

    def _infer_mime_type(self, value: Any) -> str:
        """Infer the MIME content type from the value.

        Args:
            value: The value to infer the content type from.

        Returns:
            The inferred MIME content type.
        """
        # If the value is bytes (or bytearray), return binary content type.
        if isinstance(value, (bytes, bytearray)):
            return 'application/octet-stream'

        # If the value is a dict or list, assume JSON.
        if isinstance(value, (dict, list)):
            return 'application/json; charset=utf-8'

        # If the value is a string, assume plain text.
        if isinstance(value, str):
            return 'text/plain; charset=utf-8'

        # Default fallback.
        return 'application/octet-stream'

    def _infer_filename(self, key: str, content_type: str) -> str:
        """Infer the filename from the key and content type.

        Args:
            key: The key of the record.
            content_type: The MIME content type.

        Returns:
            The inferred filename.
        """
        if content_type != 'application/octet-stream':
            ext = mimetypes.guess_extension(content_type)
            if ext and not key.endswith(ext):
                return f'{key}{ext}'
        return key
