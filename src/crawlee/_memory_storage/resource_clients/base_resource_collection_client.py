from __future__ import annotations

from abc import ABC, abstractmethod
from operator import itemgetter
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from crawlee._memory_storage.resource_clients.base_resource_client import BaseResourceClient
from crawlee._utils.file import update_metadata
from crawlee._utils.list_page import ListPage

if TYPE_CHECKING:
    from crawlee._memory_storage.memory_storage_client import MemoryStorageClient


ResourceClientType = TypeVar('ResourceClientType', bound=BaseResourceClient, contravariant=True)  # noqa: PLC0105


class BaseResourceCollectionClient(ABC, Generic[ResourceClientType]):
    """Base class for resource collection clients."""

    _base_storage_directory: str
    _memory_storage_client: MemoryStorageClient

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
    ) -> None:
        """Initialize the DatasetCollectionClient with the passed arguments."""
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client

    @abstractmethod
    def _get_storage_client_cache(self) -> BaseResourceClient:
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    def _get_resource_client_class(self) -> BaseResourceClient:
        raise NotImplementedError('You must override this method in the subclass!')

    @abstractmethod
    async def list(self) -> ListPage:
        """List the available storages.

        Returns:
            ListPage: The list of available storages matching the specified filters.
        """
        storage_client_cache = self._get_storage_client_cache()

        items = [storage.to_resource_info() for storage in storage_client_cache]

        return ListPage(
            {
                'total': len(items),
                'count': len(items),
                'offset': 0,
                'limit': len(items),
                'desc': False,
                'items': sorted(items, key=itemgetter('createdAt')),
            }
        )

    @abstractmethod
    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id_: str | None = None,
    ) -> dict:
        """Retrieve a named storage, or create a new one when it doesn't exist.

        Args:
            name: The name of the storage to retrieve or create.
            schema: The schema of the storage
            id_: The id of the storage to retrieve or create.

        Returns:
            dict: The retrieved or newly-created storage.
        """
        resource_client_class = self._get_resource_client_class()
        storage_client_cache = self._get_storage_client_cache()

        if name or id_:
            found = resource_client_class.find_or_create_client_by_id_or_name(
                memory_storage_client=self._memory_storage_client,
                name=name,
                id_=id_,
            )
            if found:
                return found.to_resource_info()

        new_resource = resource_client_class(
            id=id_,
            name=name,
            base_storage_directory=self._base_storage_directory,
            memory_storage_client=self._memory_storage_client,
        )
        storage_client_cache.append(new_resource)

        resource_info = new_resource.to_resource_info()

        # Write to the disk
        await update_metadata(
            data=resource_info,
            entity_directory=new_resource._resource_directory,  # noqa: SLF001
            write_metadata=self._memory_storage_client.write_metadata,
        )

        return cast(dict, resource_info)
