from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import Self

    from crawlee._memory_storage.memory_storage_client import MemoryStorageClient


class BaseResourceClient(ABC):
    """Base class for resource clients."""

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id_: str | None = None,
        name: str | None = None,
        resource_directory: str | None = None,
    ) -> None:
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client
        self._id = id_
        self._name = name
        self._resource_directory = resource_directory

    @abstractmethod
    async def get(self) -> dict | None:
        """Retrieve the storage.

        Returns:
            The retrieved storage, or None, if it does not exist
        """
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    def _get_storage_dir(self) -> str:
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    def _get_storage_client_cache(self) -> list[Self]:
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    def to_resource_info(self) -> dict:
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    def _create_from_directory(
        self,
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id_: str | None = None,
        name: str | None = None,
    ) -> Self:
        raise NotImplementedError('You must override this method in the subclass!')

    def find_or_create_client_by_id_or_name(  # noqa: PLR0912
        self,
        memory_storage_client: MemoryStorageClient,
        id_: str | None = None,
        name: str | None = None,
    ) -> Self | None:
        if not (isinstance(id_, str) and id_) and not (isinstance(name, str) and name):
            raise ValueError('Either "id_" or "name" must be provided and must be a non-empty string.')

        storage_client_cache = self._get_storage_client_cache(memory_storage_client)
        storages_dir = self._get_storage_dir(memory_storage_client)

        # First check memory cache
        found = next(
            (
                storage_client
                for storage_client in storage_client_cache
                if (
                    storage_client._id == id_  # noqa: SLF001
                    or (storage_client._name and name and storage_client._name.lower() == name.lower())  # noqa: SLF001
                )
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
                if id_ and id_ == metadata.get('id'):
                    storage_path = entry.path
                    name = metadata.get(name)
                    break
                if name and name == metadata.get('name'):
                    storage_path = entry.path
                    id_ = metadata.get(id_)
                    break

        # As a last resort, try to check if the accessed storage is the default one,
        # and the folder has no metadata
        # TODO: make this respect the APIFY_DEFAULT_XXX_ID env var
        # https://github.com/apify/apify-sdk-python/issues/149
        if id_ == 'default':
            possible_storage_path = os.path.join(storages_dir, id_)
            if os.access(possible_storage_path, os.F_OK):
                storage_path = possible_storage_path

        if not storage_path:
            return None

        resource_client = self._create_from_directory(storage_path, memory_storage_client, id_, name)
        storage_client_cache.append(resource_client)

        return resource_client
