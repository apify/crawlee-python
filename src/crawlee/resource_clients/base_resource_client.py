from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import Self

    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storages.models import BaseStorageMetadata


class BaseResourceClient(ABC):
    """Base class for resource clients."""

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,
        name: str | None = None,
    ) -> None:
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client
        self.id = id
        self.name = name

    @property
    @abstractmethod
    def resource_info(self) -> BaseStorageMetadata:
        """Get the resource info for the storage client."""

    @abstractmethod
    async def get(self) -> BaseStorageMetadata | None:
        """Get the info about the storage client.

        Returns:
            The storage client info or None if it does not exist.
        """

    @classmethod
    @abstractmethod
    def _get_storages_dir(cls, memory_storage_client: MemoryStorageClient) -> str:
        """Get the directory where the storage clients are stored."""

    @classmethod
    @abstractmethod
    def _get_storage_client_cache(cls, memory_storage_client: MemoryStorageClient) -> list[Self]:
        """Get the storage client cache."""

    @classmethod
    @abstractmethod
    def _create_from_directory(
        cls,
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,
        name: str | None = None,
    ) -> Self:
        """Create a new resource client from a directory."""

    @classmethod
    def find_or_create_client_by_id_or_name(
        cls,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,
        name: str | None = None,
    ) -> Self | None:
        """Locates or creates a new storage client based on the given ID or name.

        This method attempts to find a storage client in the memory cache first. If not found,
        it tries to locate a storage directory by name. If still not found, it searches through
        storage directories for a matching ID or name in their metadata. If none exists, and the
        specified ID is 'default', it checks for a default storage directory. If a storage client
        is found or created, it is added to the memory cache. If no storage client can be located or
        created, the method returns None.

        Args:
            memory_storage_client: The memory storage client used to store and retrieve storage clients.
            id: The unique identifier for the storage client. Defaults to None.
            name: The name of the storage client. Defaults to None.

        Raises:
            ValueError: If both id and name are None.

        Returns:
            The found or created storage client, or None if no client could be found or created.
        """
        if id is None and name is None:
            raise ValueError('Either id or name must be specified.')

        storage_client_cache = cls._get_storage_client_cache(memory_storage_client)
        storages_dir = cls._get_storages_dir(memory_storage_client)

        # First check memory cache
        found = next(
            (
                storage_client
                for storage_client in storage_client_cache
                if storage_client.id == id
                or (storage_client.name and name and storage_client.name.lower() == name.lower())
            ),
            None,
        )

        if found is not None:
            return found

        storage_path = None

        # First try to find the storage by looking up the directory by name
        if name:
            possible_storage_path = os.path.join(storages_dir, name)
            if os.access(possible_storage_path, os.F_OK):
                storage_path = possible_storage_path

        # If it's not found, try going through the storages dir and finding it by metadata
        if not storage_path and os.access(storages_dir, os.F_OK):
            for entry in os.scandir(storages_dir):
                if not entry.is_dir():
                    continue
                metadata_path = os.path.join(entry.path, '__metadata__.json')
                if not os.access(metadata_path, os.F_OK):
                    continue
                with open(metadata_path, encoding='utf-8') as metadata_file:
                    metadata = json.load(metadata_file)
                if id and id == metadata.get('id'):
                    storage_path = entry.path
                    name = metadata.get(name)
                    break
                if name and name == metadata.get('name'):
                    storage_path = entry.path
                    id = metadata.get(id)
                    break

        # As a last resort, try to check if the accessed storage is the default one,
        # and the folder has no metadata
        # TODO: make this respect the APIFY_DEFAULT_XXX_ID env var
        # https://github.com/apify/apify-sdk-python/issues/149
        if id == 'default':
            possible_storage_path = os.path.join(storages_dir, id)
            if os.access(possible_storage_path, os.F_OK):
                storage_path = possible_storage_path

        if not storage_path:
            return None

        resource_client = cls._create_from_directory(storage_path, memory_storage_client, id, name)
        storage_client_cache.append(resource_client)
        return resource_client
