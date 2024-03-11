from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._memory_storage import MemoryStorageClient

if TYPE_CHECKING:
    from apify_client import ApifyClientAsync


class StorageClientManager:
    """A class for managing storage clients."""

    _persist_storage: bool | None = None
    _is_at_home: bool | None = None

    _local_client: MemoryStorageClient | None = None
    _cloud_client: ApifyClientAsync | None = None

    _default_instance: StorageClientManager | None = None

    @classmethod
    def get_storage_client(
        cls: type[StorageClientManager],
        force_cloud: bool = False,  # noqa: FBT001, FBT002
    ) -> ApifyClientAsync | MemoryStorageClient:
        """Get the current storage client instance.

        Returns:
            ApifyClientAsync or MemoryStorageClient: The current storage client instance.
        """
        default_instance = cls._get_default_instance()
        if not default_instance._local_client:
            default_instance._local_client = MemoryStorageClient(
                persist_storage=default_instance._persist_storage, write_metadata=True
            )

        if default_instance._is_at_home or force_cloud:
            assert default_instance._cloud_client is not None  # noqa: S101
            return default_instance._cloud_client

        return default_instance._local_client

    @classmethod
    def set_cloud_client(cls: type[StorageClientManager], client: ApifyClientAsync) -> None:
        """Set the storage client.

        Args:
            client (ApifyClientAsync or MemoryStorageClient): The instance of a storage client.
        """
        cls._get_default_instance()._cloud_client = client

    @classmethod
    def _get_default_instance(cls: type[StorageClientManager]) -> StorageClientManager:
        if cls._default_instance is None:
            cls._default_instance = cls()

        return cls._default_instance
