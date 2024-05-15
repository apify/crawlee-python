from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.memory_storage_client import MemoryStorageClient

if TYPE_CHECKING:
    from crawlee.base_storage_client import BaseStorageClient


class StorageClientManager:
    """A class for managing storage clients."""

    _local_client: BaseStorageClient = MemoryStorageClient()
    _cloud_client: BaseStorageClient | None = None

    @classmethod
    def get_storage_client(cls, *, in_cloud: bool = False) -> BaseStorageClient:
        """Get the storage client instance for the current environment.

        Args:
            in_cloud: Whether the code is running in the cloud environment.

        Returns:
            The current storage client instance.
        """
        if in_cloud:
            if cls._cloud_client is None:
                raise RuntimeError('Running in cloud environment, but cloud client was not provided.')
            return cls._cloud_client

        return cls._local_client

    @classmethod
    def set_cloud_client(cls, cloud_client: BaseStorageClient) -> None:
        """Set the cloud storage client instance.

        Args:
            cloud_client: The cloud storage client instance.
        """
        cls._cloud_client = cloud_client

    @classmethod
    def set_local_client(cls, local_client: BaseStorageClient) -> None:
        """Set the local storage client instance.

        Args:
            local_client: The local storage client instance.
        """
        cls._local_client = local_client
