from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from apify_shared.utils import ignore_docs

from apify._memory_storage import MemoryStorageClient
from apify._memory_storage.resource_clients import BaseResourceClient, BaseResourceCollectionClient
from apify.config import Configuration
from apify.storages.storage_client_manager import StorageClientManager

if TYPE_CHECKING:
    from apify_client import ApifyClientAsync

BaseResourceClientType = TypeVar('BaseResourceClientType', bound=BaseResourceClient)
BaseResourceCollectionClientType = TypeVar('BaseResourceCollectionClientType', bound=BaseResourceCollectionClient)


@ignore_docs
class BaseStorage(ABC, Generic[BaseResourceClientType, BaseResourceCollectionClientType]):
    """A class for managing storages."""

    _id: str
    _name: str | None
    _storage_client: ApifyClientAsync | MemoryStorageClient
    _config: Configuration

    _cache_by_id: dict | None = None
    _cache_by_name: dict | None = None
    _storage_creating_lock: asyncio.Lock | None = None

    def __init__(
        self: BaseStorage,
        id: str,  # noqa: A002
        name: str | None,
        client: ApifyClientAsync | MemoryStorageClient,
        config: Configuration,
    ) -> None:
        """Initialize the storage.

        Do not use this method directly, but use `Actor.open_<STORAGE>()` instead.

        Args:
            id (str): The storage id
            name (str, optional): The storage name
            client (ApifyClientAsync or MemoryStorageClient): The storage client
            config (Configuration): The configuration
        """
        self._id = id
        self._name = name
        self._storage_client = client
        self._config = config

    @classmethod
    @abstractmethod
    def _get_human_friendly_label(cls: type[BaseStorage]) -> str:
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    @abstractmethod
    def _get_default_id(cls: type[BaseStorage], config: Configuration) -> str:
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    @abstractmethod
    def _get_single_storage_client(
        cls: type[BaseStorage],
        id: str,  # noqa: A002
        client: ApifyClientAsync | MemoryStorageClient,
    ) -> BaseResourceClientType:
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    @abstractmethod
    def _get_storage_collection_client(
        cls: type[BaseStorage],
        client: ApifyClientAsync | MemoryStorageClient,
    ) -> BaseResourceCollectionClientType:
        raise NotImplementedError('You must override this method in the subclass!')

    @classmethod
    def _ensure_class_initialized(cls: type[BaseStorage]) -> None:
        if cls._cache_by_id is None:
            cls._cache_by_id = {}
        if cls._cache_by_name is None:
            cls._cache_by_name = {}
        if cls._storage_creating_lock is None:
            cls._storage_creating_lock = asyncio.Lock()

    @classmethod
    @abstractmethod
    async def open(
        cls: type[BaseStorage],
        *,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
        force_cloud: bool = False,
        config: Configuration | None = None,
    ) -> BaseStorage:
        """Open a storage, or return a cached storage object if it was opened before.

        Opens a storage with the given ID or name.
        Returns the cached storage object if the storage was opened before.

        Args:
            id (str, optional): ID of the storage to be opened.
                If neither `id` nor `name` are provided, the method returns the default storage associated with the actor run.
                If the storage with the given ID does not exist, it raises an error.
            name (str, optional): Name of the storage to be opened.
                If neither `id` nor `name` are provided, the method returns the default storage associated with the actor run.
                If the storage with the given name does not exist, it is created.
            force_cloud (bool, optional): If set to True, it will open a storage on the Apify Platform even when running the actor locally.
                Defaults to False.
            config (Configuration, optional): A `Configuration` instance, uses global configuration if omitted.

        Returns:
            An instance of the storage.
        """
        cls._ensure_class_initialized()
        assert cls._cache_by_id is not None  # noqa: S101
        assert cls._cache_by_name is not None  # noqa: S101
        assert not (id and name)  # noqa: S101

        used_config = config or Configuration.get_global_configuration()
        used_client = StorageClientManager.get_storage_client(force_cloud=force_cloud)

        is_default_storage_on_local = False
        # Fetch default ID if no ID or name was passed
        if not id and not name:
            if isinstance(used_client, MemoryStorageClient):
                is_default_storage_on_local = True
            id = cls._get_default_id(used_config)  # noqa: A001

        # Try to get the storage instance from cache
        cached_storage = None
        if id:
            cached_storage = cls._cache_by_id.get(id)
        elif name:
            cached_storage = cls._cache_by_name.get(name)

        if cached_storage is not None:
            # This cast is needed since MyPy doesn't understand very well that Self and Storage are the same
            return cast(BaseStorage, cached_storage)

        # Purge default storages if configured
        if used_config.purge_on_start and isinstance(used_client, MemoryStorageClient):
            await used_client._purge_on_start()

        assert cls._storage_creating_lock is not None  # noqa: S101
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
                storage_info = await storage_collection_client.get_or_create(name=name, _id=id)
            else:
                storage_collection_client = cls._get_storage_collection_client(used_client)
                storage_info = await storage_collection_client.get_or_create(name=name)

            storage = cls(storage_info['id'], storage_info.get('name'), used_client, used_config)

            # Cache by id and name
            cls._cache_by_id[storage._id] = storage
            if storage._name is not None:
                cls._cache_by_name[storage._name] = storage

        return storage

    def _remove_from_cache(self: BaseStorage) -> None:
        if self.__class__._cache_by_id is not None:
            del self.__class__._cache_by_id[self._id]

        if self._name and self.__class__._cache_by_name is not None:
            del self.__class__._cache_by_name[self._name]
