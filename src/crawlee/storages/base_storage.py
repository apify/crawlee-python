from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, cast

from crawlee.config import Config
from crawlee.memory_storage import MemoryStorageClient
from crawlee.memory_storage.resource_clients import BaseResourceClient, BaseResourceCollectionClient
from crawlee.storages.storage_client_manager import StorageClientManager

BaseResourceClientType = TypeVar('BaseResourceClientType', bound=BaseResourceClient)
BaseResourceCollectionClientType = TypeVar('BaseResourceCollectionClientType', bound=BaseResourceCollectionClient)


class BaseStorage(ABC, Generic[BaseResourceClientType, BaseResourceCollectionClientType]):
    """A class for managing storages."""

    _purge_on_start: bool
    cache_by_id: dict | None = None
    cache_by_name: dict | None = None
    _storage_creating_lock: asyncio.Lock | None = None

    def __init__(
        self: BaseStorage,
        id_: str,
        name: str | None,
        config: Config,
        client: MemoryStorageClient,
    ) -> None:
        """Create a new instance.

        Args:
            id_: The storage id
            name: The storage name
            config: The configuration
            client: The storage client
        """
        self.id = id_
        self.name = name
        self._config = config
        self._storage_client = client

    @classmethod
    async def open(
        cls: type[BaseStorage],
        *,
        config: Config | None = None,
        id_: str | None = None,
        name: str | None = None,
    ) -> BaseStorage:
        """Opens a storage instance based on provided identifiers or returns a previously opened instance from cache.

        This method facilitates the retrieval or initialization of a storage instance using specified configuration
        details. If an instance corresponding to the given `id` or `name` was previously opened, this method returns
        the cached instance. Otherwise, it initializes a new storage instance. When neither `id` nor `name` is provided,
        the default storage associated with the current actor run is utilized.

        Args:
            id_: Identifier for the specific storage to open. An error is raised if no matching storage is found.
            name: Name for the specific storage to open or create.
            config: Configuration instance to use. If omitted, the global configuration is applied.

        Returns:
            The opened or retrieved storage instance.
        """
        cls._ensure_class_initialized()

        if cls.cache_by_id is None:
            raise AttributeError("The 'cache_by_id' attribute must not be None.")

        if cls.cache_by_name is None:
            raise AttributeError("The 'cache_by_name' attribute must not be None.")

        if id_ and name:
            raise ValueError("Either 'id_' or 'name' must be provided, not both.")

        used_config = config or Config()
        used_client = StorageClientManager.get_storage_client()

        is_default_storage_on_local = False
        # Fetch default ID if no ID or name was passed
        if not id_ and not name:
            if isinstance(used_client, MemoryStorageClient):
                is_default_storage_on_local = True
            id_ = cls._get_default_id(used_config)

        # Try to get the storage instance from cache
        cached_storage = None
        if id_:
            cached_storage = cls.cache_by_id.get(id_)
        elif name:
            cached_storage = cls.cache_by_name.get(name)

        if cached_storage is not None:
            # This cast is needed since MyPy doesn't understand very well that Self and Storage are the same
            return cast(BaseStorage, cached_storage)

        # Purge default storages if configured
        if used_config.purge_on_start and isinstance(used_client, MemoryStorageClient):
            await used_client.purge_on_start()

        assert cls._storage_creating_lock is not None  # noqa: S101
        async with cls._storage_creating_lock:
            # Create the storage
            if id_ and not is_default_storage_on_local:
                single_storage_client = cls._get_single_storage_client(id_, used_client)
                storage_info = await single_storage_client.get()
                if not storage_info:
                    storage_label = cls._get_human_friendly_label()
                    raise RuntimeError(f'{storage_label} with id "{id_}" does not exist!')
            elif is_default_storage_on_local:
                storage_collection_client = cls._get_storage_collection_client(used_client)
                storage_info = await storage_collection_client.get_or_create(name=name, id_=id_)
            else:
                storage_collection_client = cls._get_storage_collection_client(used_client)
                storage_info = await storage_collection_client.get_or_create(name=name)

            storage = cls(
                id_=storage_info['id'],
                name=storage_info.get('name'),
                config=used_config,
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
        raise NotImplementedError('The subclass must implement this method.')

    @classmethod
    @abstractmethod
    def _get_default_id(cls, config: Config) -> str:
        raise NotImplementedError('The subclass must implement this method.')

    @classmethod
    @abstractmethod
    def _get_single_storage_client(cls, id_: str, client: MemoryStorageClient) -> BaseResourceClientType:
        raise NotImplementedError('The subclass must implement this method.')

    @classmethod
    @abstractmethod
    def _get_storage_collection_client(cls, client: MemoryStorageClient) -> BaseResourceCollectionClientType:
        raise NotImplementedError('The subclass must implement this method.')

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
            del self.__class__.cache_by_id[self.id]

        if self.name and self.__class__.cache_by_name is not None:
            del self.__class__.cache_by_name[self.name]
