from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from ._dataset_client import DatasetClient
    from ._key_value_store_client import KeyValueStoreClient
    from ._request_queue_client import RequestQueueClient


class StorageClient(ABC):
    """Base class for storage clients."""

    @abstractmethod
    async def open_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None,
    ) -> DatasetClient:
        """Open the dataset client."""

    @abstractmethod
    async def open_key_value_store_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None,
    ) -> KeyValueStoreClient:
        """Open the key-value store client."""

    @abstractmethod
    async def open_request_queue_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None,
    ) -> RequestQueueClient:
        """Open the request queue client."""
