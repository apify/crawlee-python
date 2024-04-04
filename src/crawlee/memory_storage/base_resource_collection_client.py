from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from crawlee._utils.file import persist_metadata_if_enabled
from crawlee.memory_storage.base_resource_client import BaseResourceClient
from crawlee.storages.types import BaseResourceInfo, ListPage

if TYPE_CHECKING:
    from crawlee.memory_storage.memory_storage_client import MemoryStorageClient

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

    async def list(self) -> ListPage:
        """List the available storages.

        Returns:
            The list of available storages matching the specified filters.
        """
        storage_client_cache = self._get_storage_client_cache()

        items = [storage.resource_info for storage in storage_client_cache]

        return ListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,  # noqa: ARG002
        id_: str | None = None,
    ) -> BaseResourceInfo:
        """Retrieve a named storage, or create a new one when it doesn't exist.

        Args:
            name: The name of the storage to retrieve or create.
            schema: The schema of the storage
            id_: ID of the storage to retrieve or create

        Returns:
            The retrieved or newly-created storage.
        """
        resource_client_class = self._client_class
        storage_client_cache = self._get_storage_client_cache()

        if name or id_:
            found = resource_client_class.find_or_create_client_by_id_or_name(
                memory_storage_client=self._memory_storage_client,
                name=name,
                id_=id_,
            )
            if found:
                return found.resource_info

        new_resource = resource_client_class(
            id_=id_,
            name=name,
            base_storage_directory=self._base_storage_directory,
            memory_storage_client=self._memory_storage_client,
        )
        storage_client_cache.append(new_resource)

        resource_info = new_resource.resource_info
        data = resource_info.__dict__ if isinstance(resource_info, BaseResourceInfo) else resource_info

        # Write to the disk
        await persist_metadata_if_enabled(
            data=data,
            entity_directory=new_resource.resource_directory,  # type: ignore
            write_metadata=self._memory_storage_client.write_metadata,
        )

        return resource_info
