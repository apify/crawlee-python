from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from apify_shared.utils import ignore_docs

if TYPE_CHECKING:
    from typing_extensions import Self

    from apify._memory_storage.memory_storage_client import MemoryStorageClient


@ignore_docs
class BaseResourceClient(ABC):
    """Base class for resource clients."""

    _id: str
    _name: str | None
    _resource_directory: str

    @abstractmethod
    def __init__(
        self: BaseResourceClient,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
    ) -> None:
        """Initialize the BaseResourceClient."""
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    async def get(self: BaseResourceClient) -> dict | None:
        """Retrieve the storage.

        Returns:
            dict, optional: The retrieved storage, or None, if it does not exist
        """
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    @abstractmethod
    def _get_storages_dir(cls: type[BaseResourceClient], memory_storage_client: MemoryStorageClient) -> str:
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    @abstractmethod
    def _get_storage_client_cache(
        cls,  # noqa: ANN102 # type annotated cls does not work with Self as a return type
        memory_storage_client: MemoryStorageClient,
    ) -> list[Self]:
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    def _to_resource_info(self: BaseResourceClient) -> dict:
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    @abstractmethod
    def _create_from_directory(
        cls,  # noqa: ANN102 # type annotated cls does not work with Self as a return type
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
    ) -> Self:
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    def _find_or_create_client_by_id_or_name(
        cls,  # noqa: ANN102 # type annotated cls does not work with Self as a return type
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
    ) -> Self | None:
        assert id is not None or name is not None  # noqa: S101

        storage_client_cache = cls._get_storage_client_cache(memory_storage_client)
        storages_dir = cls._get_storages_dir(memory_storage_client)

        # First check memory cache
        found = next(
            (
                storage_client
                for storage_client in storage_client_cache
                if storage_client._id == id or (storage_client._name and name and storage_client._name.lower() == name.lower())
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
                    id = metadata.get(id)  # noqa: A001
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
