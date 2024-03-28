from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.memory_storage import MemoryStorageClient

if TYPE_CHECKING:
    from apify_client import ApifyClientAsync


class StorageClientManager:
    """A class for managing storage clients."""

    persist_storage: bool | None = None
    is_at_home: bool | None = None

    local_client: MemoryStorageClient | None = None
    cloud_client: ApifyClientAsync | None = None

    _default_instance: StorageClientManager | None = None

    @classmethod
    def get_storage_client(cls) -> MemoryStorageClient:
        """Get the current storage client instance.

        Returns:
            ApifyClientAsync or MemoryStorageClient: The current storage client instance.
        """
        default_instance = cls._get_default_instance()
        if not default_instance.local_client:
            default_instance.local_client = MemoryStorageClient(
                persist_storage=default_instance.persist_storage, write_metadata=True
            )

        if default_instance.is_at_home:
            if default_instance.cloud_client is None:
                raise RuntimeError('Cloud client is expected but not set in Apify Cloud environment.')

            return default_instance.cloud_client  # type: ignore

        return default_instance.local_client

    @classmethod
    def set_cloud_client(cls, client: ApifyClientAsync) -> None:
        """Set the storage client.

        Args:
            client: The instance of a storage client.
        """
        cls._get_default_instance().cloud_client = client

    @classmethod
    def _get_default_instance(cls) -> StorageClientManager:
        if cls._default_instance is None:
            cls._default_instance = cls()

        return cls._default_instance
