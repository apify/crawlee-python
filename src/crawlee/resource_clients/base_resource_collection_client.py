from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from crawlee._utils.file import persist_metadata_if_enabled
from crawlee.resource_clients.base_resource_client import BaseResourceClient

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storages.models import BaseListPage, BaseStorageMetadata

ResourceClientType = TypeVar('ResourceClientType', bound=BaseResourceClient, contravariant=True)  # noqa: PLC0105


class BaseResourceCollectionClient(ABC, Generic[ResourceClientType]):
    """Base class for resource collection clients."""

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
    ) -> None:
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client

    @property
    @abstractmethod
    def _client_class(self) -> type[ResourceClientType]:
        """Get the class of the resource clients."""

    @abstractmethod
    def _get_storage_client_cache(self) -> list[ResourceClientType]:
        """Get the storage client cache."""

    @abstractmethod
    async def list(self) -> BaseListPage:
        """List the available storages.

        Returns:
            The list of available storages matching the specified filters.
        """

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,  # noqa: ARG002
        id: str | None = None,
    ) -> BaseStorageMetadata:
        """Retrieve a named storage, or create a new one when it doesn't exist.

        Args:
            name: The name of the storage to retrieve or create.
            schema: The schema of the storage
            id: ID of the storage to retrieve or create

        Returns:
            The retrieved or newly-created storage.
        """
        resource_client_class = self._client_class
        storage_client_cache = self._get_storage_client_cache()

        if name or id:
            found = resource_client_class.find_or_create_client_by_id_or_name(
                memory_storage_client=self._memory_storage_client,
                name=name,
                id=id,
            )
            if found:
                return found.resource_info

        new_resource = resource_client_class(
            id=id,
            name=name,
            base_storage_directory=self._base_storage_directory,
            memory_storage_client=self._memory_storage_client,
        )
        storage_client_cache.append(new_resource)

        # Write to the disk
        await persist_metadata_if_enabled(
            data=new_resource.resource_info.model_dump(),
            entity_directory=new_resource.resource_directory,  # type: ignore
            write_metadata=self._memory_storage_client.write_metadata,
        )

        return new_resource.resource_info
