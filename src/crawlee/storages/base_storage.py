from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from logging import getLogger
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from typing_extensions import Self

from crawlee.configuration import Configuration

if TYPE_CHECKING:
    from crawlee.resource_clients.base_resource_client import BaseResourceClient
    from crawlee.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
    from crawlee.storage_clients import MemoryStorageClient

    BaseResourceClientType = TypeVar('BaseResourceClientType', bound=BaseResourceClient)
    BaseResourceCollectionClientType = TypeVar('BaseResourceCollectionClientType', bound=BaseResourceCollectionClient)
else:
    BaseResourceClientType = TypeVar('BaseResourceClientType')
    BaseResourceCollectionClientType = TypeVar('BaseResourceCollectionClientType')

logger = getLogger(__name__)


class BaseStorage(ABC, Generic[BaseResourceClientType, BaseResourceCollectionClientType]):
    """A class for managing storages."""

    _purge_on_start: bool
    cache_by_id: dict | None = None
    cache_by_name: dict | None = None
    _storage_creating_lock: asyncio.Lock | None = None

    def __init__(
        self,
        id: str,
        name: str | None,
        configuration: Configuration,
        client: MemoryStorageClient,
    ) -> None:
        """Create a new instance.

        Args:
            id: ID of the storage.
            name: Name of the storage.
            configuration: The configuration settings.
            client: The underlying storage client to be used.
        """
        self.id = id
        self._name = name
        self._configuration = configuration
        self._storage_client = client

    @property
    def name(self) -> str | None:
        """Name of the storage."""
        return self._name

    @classmethod
    async def open(
        cls,
        *,
        configuration: Configuration | None = None,
        id: str | None = None,
        name: str | None = None,
    ) -> Self:
        """Opens a storage instance based on provided identifiers or returns a previously opened instance from cache.

        This method facilitates the retrieval or initialization of a storage instance using specified configuration
        details. If an instance corresponding to the given `id` or `name` was previously opened, this method returns
        the cached instance. Otherwise, it initializes a new storage instance. When neither `id` nor `name` is provided,
        the default storage associated with the current actor run is utilized.

        Args:
            id: Identifier for the specific storage to open. An error is raised if no matching storage is found.
            name: Name for the specific storage to open or create.
            configuration: Configuration instance to use. If omitted, the global configuration is applied.

        Returns:
            The opened or retrieved storage instance.
        """
        from crawlee.storage_client_manager import StorageClientManager
        from crawlee.storage_clients import MemoryStorageClient

        cls._ensure_class_initialized()

        if cls.cache_by_id is None:
            raise AttributeError("The 'cache_by_id' attribute must not be None.")

        if cls.cache_by_name is None:
            raise AttributeError("The 'cache_by_name' attribute must not be None.")

        if id and name:
            raise ValueError("Either 'id' or 'name' must be provided, not both.")

        used_config = configuration or Configuration()
        used_client = StorageClientManager.get_storage_client()

        is_default_storage_on_local = False
        # Fetch default ID if no ID or name was passed
        if not id and not name:
            if isinstance(used_client, MemoryStorageClient):
                is_default_storage_on_local = True
            id = cls._get_default_id(used_config)

        # Try to get the storage instance from cache
        cached_storage = None
        if id:
            cached_storage = cls.cache_by_id.get(id)
        elif name:
            cached_storage = cls.cache_by_name.get(name)

        if cached_storage is not None:
            # This cast is needed since MyPy doesn't understand very well that Self and Storage are the same
            return cast(Self, cached_storage)

        # Purge default storages if configured
        if used_config.purge_on_start and isinstance(used_client, MemoryStorageClient):
            await used_client.purge_on_start()

        if cls._storage_creating_lock is None:
            raise AttributeError('cls._storage_creating_lock must be initialized before calling open.')

        async with cls._storage_creating_lock:
            # Create the storage
            if id and not is_default_storage_on_local:
                single_storage_client = cls._get_single_storage_client(id, used_client)
                storage_info = await single_storage_client.get()
                if not storage_info:
                    storage_label = cls._get_human_friendly_label()
                    raise RuntimeError(f'{storage_label} with id "{id}" does not exist!')
            elif is_default_storage_on_local:
                storage_collection_client = cls._get_storage_collection_client(used_client)
                storage_info = await storage_collection_client.get_or_create(name=name, id=id)
            else:
                storage_collection_client = cls._get_storage_collection_client(used_client)
                storage_info = await storage_collection_client.get_or_create(name=name)

            storage = cls(
                id=storage_info.id,
                name=storage_info.name,
                configuration=used_config,
                client=used_client,
            )

            # Cache by id and name
            cls.cache_by_id[storage.id] = storage
            if storage.name is not None:
                cls.cache_by_name[storage.name] = storage

        return storage

    @classmethod
    @abstractmethod
    def _get_human_friendly_label(cls) -> str:
        """Get a human-friendly label for the storage."""

    @classmethod
    @abstractmethod
    def _get_default_id(cls, configuration: Configuration) -> str:
        """Get the default storage ID."""

    @classmethod
    @abstractmethod
    def _get_single_storage_client(cls, id: str, client: MemoryStorageClient) -> BaseResourceClientType:
        """Get the single storage client for the given ID."""

    @classmethod
    @abstractmethod
    def _get_storage_collection_client(cls, client: MemoryStorageClient) -> BaseResourceCollectionClientType:
        """Get the storage collection client."""

    @classmethod
    def _ensure_class_initialized(cls) -> None:
        if cls.cache_by_id is None:
            cls.cache_by_id = {}
        if cls.cache_by_name is None:
            cls.cache_by_name = {}
        if cls._storage_creating_lock is None:
            cls._storage_creating_lock = asyncio.Lock()

    def _remove_from_cache(self) -> None:
        if self.__class__.cache_by_id is not None:
            try:
                del self.__class__.cache_by_id[self.id]
            except KeyError as exc:
                raise RuntimeError(f'Storage with provided ID was not found ({self.id}).') from exc

        if self.name and self.__class__.cache_by_name is not None:
            try:
                del self.__class__.cache_by_name[self.name]
            except KeyError as exc:
                raise RuntimeError(f'Storage with provided name was not found ({self.name}).') from exc
