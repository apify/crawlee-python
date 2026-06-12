from __future__ import annotations

import json
from logging import getLogger
from typing import TYPE_CHECKING, Any

from crawlee_storage import FileSystemKeyValueStoreClient as NativeKeyValueStoreClient
from typing_extensions import Self, override

from crawlee._utils.file import infer_mime_type, json_dumps
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

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

    Backed by the native ``crawlee_storage`` Rust extension for performance.
    """

    def __init__(
        self,
        *,
        native_client: NativeKeyValueStoreClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemKeyValueStoreClient.open` class method to create a new instance.
        """
        self._native_client = native_client

    @property
    def path_to_kvs(self) -> Path:
        """The full path to the key-value store directory."""
        return self._native_client.path_to_kvs

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the key-value store metadata file."""
        return self._native_client.path_to_metadata

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        raw = await self._native_client.get_metadata()
        return KeyValueStoreMetadata.model_validate(raw)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        configuration: Configuration,
    ) -> Self:
        """Open or create a file system key-value store client.

        This method attempts to open an existing key-value store from the file system. If a KVS with the specified
        ID or name exists, it loads the metadata from the stored files. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store to open. If provided, searches for existing store by ID.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.
            configuration: The configuration object containing storage directory settings.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a store with the specified ID is not found, if metadata is invalid,
                or if both name and alias are provided.
        """
        native_client = await NativeKeyValueStoreClient.open(
            id=id,
            name=name,
            alias=alias,
            storage_dir=str(configuration.storage_dir),
        )

        return cls(native_client=native_client)

    @override
    async def drop(self) -> None:
        await self._native_client.drop_storage()

    @override
    async def purge(self) -> None:
        await self._native_client.purge()

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        raw = await self._native_client.get_value(key)

        if raw is None:
            return None

        # The native client always returns the raw bytes; deserialize them back into a Python
        # object according to the stored content type to match the framework contract.
        content_type = raw['contentType']
        value_bytes: bytes = raw['value']

        if content_type == 'application/x-none':
            value: Any = None
        elif 'application/json' in content_type:
            try:
                value = json.loads(value_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(f'Failed to decode JSON value for key "{key}"')
                return None
        elif content_type.startswith('text/'):
            try:
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f'Failed to decode text value for key "{key}"')
                return None
        else:
            value = value_bytes

        return KeyValueStoreRecord(
            key=raw['key'],
            value=value,
            content_type=content_type,
            size=raw.get('size'),
        )

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        # The native client only accepts raw bytes plus a content type, so serialize the value
        # here the same way the on-disk format expects.
        if value is None:
            content_type = 'application/x-none'  # Special content type to identify None values.
            value_bytes = b''
        else:
            content_type = content_type or infer_mime_type(value)

            if 'application/json' in content_type:
                value_bytes = (await json_dumps(value)).encode('utf-8')
            elif isinstance(value, str):
                value_bytes = value.encode('utf-8')
            elif isinstance(value, (bytes, bytearray)):
                value_bytes = bytes(value)
            else:
                # Fallback: attempt to convert to string and encode.
                value_bytes = str(value).encode('utf-8')

        await self._native_client.set_value(key, value_bytes, content_type)

    @override
    async def delete_value(self, *, key: str) -> None:
        await self._native_client.delete_value(key)

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        async for item in self._native_client.iterate_keys(
            exclusive_start_key=exclusive_start_key,
            limit=limit,
        ):
            yield KeyValueStoreRecordMetadata.model_validate(item)

    @override
    async def get_public_url(self, *, key: str) -> str:
        return await self._native_client.get_public_url(key)

    @override
    async def record_exists(self, *, key: str) -> bool:
        return await self._native_client.record_exists(key)
