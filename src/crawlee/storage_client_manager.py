from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.memory_storage_client import MemoryStorageClient

if TYPE_CHECKING:
    from crawlee.base_storage_client import BaseStorageClient


class StorageClientManager:
    """A class for managing storage clients."""

    def __init__(
        self,
        *,
        local_client: BaseStorageClient | None = None,
        cloud_client: BaseStorageClient | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            local_client: The storage client to be used in the local environment.
            cloud_client: The storage client to be used in the cloud environment.
        """
        self._local_client = local_client or MemoryStorageClient()
        self._cloud_client = cloud_client

    def get_storage_client(self, *, in_cloud: bool = False) -> BaseStorageClient:
        """Get the storage client instance for the current environment.

        Args:
            in_cloud: Whether the code is running in the cloud environment.

        Returns:
            The current storage client instance.
        """
        if in_cloud:
            if self._cloud_client is None:
                raise RuntimeError('Running in cloud environment, but cloud client was not provided.')
            return self._cloud_client

        return self._local_client
