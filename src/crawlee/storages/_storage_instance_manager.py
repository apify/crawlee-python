from __future__ import annotations

from collections import defaultdict
from collections.abc import Coroutine, Hashable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar

from crawlee.storage_clients._base import DatasetClient, KeyValueStoreClient, RequestQueueClient

if TYPE_CHECKING:
    from crawlee.storage_clients import StorageClient

    from ._base import Storage

T = TypeVar('T', bound='Storage')


@dataclass
class _StorageClientCache:
    """Cache for specific storage client.

    Example:
        Storage=Dataset, id='123', additional_cache_key="some_path" will be located in
        storage =  by_id[Dataset]['123'][some_path]
    """

    by_id: defaultdict[type[Storage], defaultdict[str, defaultdict[Hashable, Storage]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))
    )
    """Cache for storage instances by ID, separated by storage type and additional hash key."""

    by_name: defaultdict[type[Storage], defaultdict[str, defaultdict[Hashable, Storage]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))
    )
    """Cache for storage instances by name, separated by storage type and additional hash key."""

    by_alias: defaultdict[type[Storage], defaultdict[str, defaultdict[Hashable, Storage]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))
    )
    """Cache for storage instances by alias, separated by storage type and additional hash key."""


StorageClientType = DatasetClient | KeyValueStoreClient | RequestQueueClient
"""Type alias for the storage client types."""

ClientOpenerCoro = Coroutine[None, None, StorageClientType]
"""Type alias for the client opener function."""


class StorageInstanceManager:
    """Manager for caching and managing storage instances.

    This class centralizes the caching logic for all storage types (Dataset, KeyValueStore, RequestQueue)
    and provides a unified interface for opening and managing storage instances.
    """

    _DEFAULT_STORAGE_ALIAS = '__default__'
    """Reserved alias for default unnamed storage."""

    def __init__(self) -> None:
        self._cache_by_storage_client: dict[type[StorageClient], _StorageClientCache] = defaultdict(_StorageClientCache)

    async def open_storage_instance(
        self,
        cls: type[T],
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        storage_client_type: type[StorageClient],
        client_opener_coro: ClientOpenerCoro,
        additional_cache_key: Hashable = '',
    ) -> T:
        """Open a storage instance with caching support.

        Args:
            cls: The storage class to instantiate.
            id: Storage ID.
            name: Storage name. (global scope, persists across runs).
            alias: Storage alias (run scope, creates unnamed storage).
            storage_client_type: Type of storage client to use.
            client_opener_coro: Coroutine to open the storage client when storage instance not found in cache.
            additional_cache_key: Additional optional key to differentiate cache entries.

        Returns:
            The storage instance.

        Raises:
            ValueError: If multiple parameters out of `id`, `name`, and `alias` are specified.
        """
        try:
            if name == self._DEFAULT_STORAGE_ALIAS:
                raise ValueError(
                    f'Storage name cannot be "{self._DEFAULT_STORAGE_ALIAS}" as it is reserved for default alias.'
                )

            # Validate input parameters.
            specified_params = sum(1 for param in [id, name, alias] if param is not None)
            if specified_params > 1:
                raise ValueError('Only one of "id", "name", or "alias" can be specified, not multiple.')

            # Auto-set alias='default' when no parameters are specified.
            # Default unnamed storage is equal to alias=default unnamed storage.
            if specified_params == 0:
                alias = self._DEFAULT_STORAGE_ALIAS

            # Check cache
            if id is not None and (
                cached_instance := self._cache_by_storage_client[storage_client_type]
                .by_id[cls][id]
                .get(additional_cache_key)
            ):
                if isinstance(cached_instance, cls):
                    return cached_instance
                raise RuntimeError('Cached instance type mismatch.')

            if name is not None and (
                cached_instance := self._cache_by_storage_client[storage_client_type]
                .by_name[cls][name]
                .get(additional_cache_key)
            ):
                if isinstance(cached_instance, cls):
                    return cached_instance
                raise RuntimeError('Cached instance type mismatch.')

            if alias is not None and (
                cached_instance := self._cache_by_storage_client[storage_client_type]
                .by_alias[cls][alias]
                .get(additional_cache_key)
            ):
                if isinstance(cached_instance, cls):
                    return cached_instance
                raise RuntimeError('Cached instance type mismatch.')

            # Check for conflicts between named and alias storages
            if alias and (
                self._cache_by_storage_client[storage_client_type].by_name[cls][alias].get(additional_cache_key)
            ):
                raise ValueError(
                    f'Cannot create alias storage "{alias}" because a named storage with the same name already exists. '
                    f'Use a different alias or drop the existing named storage first.'
                )

            if name and (
                self._cache_by_storage_client[storage_client_type].by_alias[cls][name].get(additional_cache_key)
            ):
                raise ValueError(
                    f'Cannot create named storage "{name}" because an alias storage with the same name already exists. '
                    f'Use a different name or drop the existing alias storage first.'
                )

            # Create new instance
            client: KeyValueStoreClient | DatasetClient | RequestQueueClient
            client = await client_opener_coro

            metadata = await client.get_metadata()

            instance = cls(client, metadata.id, metadata.name)  # type: ignore[call-arg]
            instance_name = getattr(instance, 'name', None)

            # Cache the instance.
            # Always cache by id.
            self._cache_by_storage_client[storage_client_type].by_id[cls][instance.id][additional_cache_key] = instance

            # Cache named storage.
            if instance_name is not None:
                self._cache_by_storage_client[storage_client_type].by_name[cls][instance_name][additional_cache_key] = (
                    instance
                )

            # Cache unnamed storage.
            if alias is not None:
                self._cache_by_storage_client[storage_client_type].by_alias[cls][alias][additional_cache_key] = instance

            return instance

        finally:
            # Make sure the client opener is closed.
            # If it was awaited, then closing is no operation, if it was not awaited, this is the cleanup.
            client_opener_coro.close()

    def remove_from_cache(self, storage_instance: Storage) -> None:
        """Remove a storage instance from the cache.

        Args:
            storage_instance: The storage instance to remove.
        """
        storage_type = type(storage_instance)

        for storage_client_cache in self._cache_by_storage_client.values():
            # Remove from ID cache
            for additional_key in storage_client_cache.by_id[storage_type][storage_instance.id]:
                del storage_client_cache.by_id[storage_type][storage_instance.id][additional_key]
                break

            # Remove from name cache or alias cache. It can never be in both.
            if storage_instance.name is not None:
                for additional_key in storage_client_cache.by_name[storage_type][storage_instance.name]:
                    del storage_client_cache.by_name[storage_type][storage_instance.name][additional_key]
                    break
            else:
                for alias_key in storage_client_cache.by_alias[storage_type]:
                    for additional_key in storage_client_cache.by_alias[storage_type][alias_key]:
                        del storage_client_cache.by_alias[storage_type][alias_key][additional_key]
                        break

    def clear_cache(self) -> None:
        """Clear all cached storage instances."""
        self._cache_by_storage_client = defaultdict(_StorageClientCache)
