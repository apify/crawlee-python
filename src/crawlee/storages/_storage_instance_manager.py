from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeVar

from crawlee.storage_clients._base import DatasetClient, KeyValueStoreClient, RequestQueueClient

from ._base import Storage

if TYPE_CHECKING:
    from crawlee.configuration import Configuration

T = TypeVar('T', bound='Storage')

StorageClientType = DatasetClient | KeyValueStoreClient | RequestQueueClient
"""Type alias for the storage client types."""

ClientOpener = Callable[..., Awaitable[StorageClientType]]
"""Type alias for the client opener function."""


class StorageInstanceManager:
    """Manager for caching and managing storage instances.

    This class centralizes the caching logic for all storage types (Dataset, KeyValueStore, RequestQueue)
    and provides a unified interface for opening and managing storage instances.
    """

    _DEFAULT_STORAGE = 'default'

    def __init__(self) -> None:
        self._cache_by_id = dict[type[Storage], dict[str, Storage]]()
        """Cache for storage instances by ID, separated by storage type."""

        self._cache_by_name = dict[type[Storage], dict[str, Storage]]()
        """Cache for storage instances by name, separated by storage type."""

        self._cache_by_alias = dict[type[Storage], dict[str, Storage]]()
        """Cache for storage instances by alias, separated by storage type."""

        self._default_instances = dict[type[Storage], Storage]()
        """Cache for default instances of each storage type."""

    async def open_storage_instance(
        self,
        cls: type[T],
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        configuration: Configuration,
        client_opener: ClientOpener,
    ) -> T:
        """Open a storage instance with caching support.

        Args:
            cls: The storage class to instantiate.
            id: Storage ID.
            name: Storage name (global scope, persists across runs).
            alias: Storage alias (run scope, creates unnamed storage).
            configuration: Configuration object.
            client_opener: Function to create the storage client.

        Returns:
            The storage instance.

        Raises:
            ValueError: If multiple parameters out of `id`, `name`, and `alias` are specified.
        """
        # Validate input parameters.
        specified_params = sum(1 for param in [id, name, alias] if param is not None)
        if specified_params > 1:
            raise ValueError('Only one of "id", "name", or "alias" can be specified, not multiple.')

        # Auto-set alias='default' when no parameters are specified.
        # Default unnamed storage is equal to alias=default unnamed storage.
        if specified_params == 0:
            alias = self._DEFAULT_STORAGE
            specified_params = 1

        # Check cache
        if id is not None:
            type_cache_by_id = self._cache_by_id.get(cls, {})
            if id in type_cache_by_id:
                cached_instance = type_cache_by_id[id]
                if isinstance(cached_instance, cls):
                    return cached_instance

        if name is not None:
            type_cache_by_name = self._cache_by_name.get(cls, {})
            if name in type_cache_by_name:
                cached_instance = type_cache_by_name[name]
                if isinstance(cached_instance, cls):
                    return cached_instance

        if alias is not None:
            type_cache_by_alias = self._cache_by_alias.get(cls, {})
            if alias in type_cache_by_alias:
                cached_instance = type_cache_by_alias[alias]
                if isinstance(cached_instance, cls):
                    return cached_instance

        # Check for conflicts between named and alias storages
        if name is not None:
            # Check if there's already an alias storage with the same identifier
            type_cache_by_alias = self._cache_by_alias.get(cls, {})
            if name in type_cache_by_alias:
                raise ValueError(
                    f'Cannot create named storage "{name}" because an alias storage with the same name already exists. '
                    f'Use a different name or drop the existing alias storage first.'
                )

        if alias is not None:
            # Check if there's already a named storage with the same identifier
            type_cache_by_name = self._cache_by_name.get(cls, {})
            if alias in type_cache_by_name:
                raise ValueError(
                    f'Cannot create alias storage "{alias}" because a named storage with the same name already exists. '
                    f'Use a different alias or drop the existing named storage first.'
                )

        # Create new instance
        # Pass the correct parameters to the storage client
        if alias is not None:
            client = await client_opener(id=id, name=None, alias=alias, configuration=configuration)
        else:
            client = await client_opener(id=id, name=name, configuration=configuration)
        metadata = await client.get_metadata()

        instance = cls(client, metadata.id, metadata.name)  # type: ignore[call-arg]
        instance_name = getattr(instance, 'name', None)

        # Cache the instance
        type_cache_by_id = self._cache_by_id.setdefault(cls, {})
        type_cache_by_name = self._cache_by_name.setdefault(cls, {})
        type_cache_by_alias = self._cache_by_alias.setdefault(cls, {})

        type_cache_by_id[instance.id] = instance
        if instance_name is not None:
            type_cache_by_name[instance_name] = instance
        if alias is not None:
            type_cache_by_alias[alias] = instance

        return instance

    def remove_from_cache(self, storage_instance: Storage) -> None:
        """Remove a storage instance from the cache.

        Args:
            storage_instance: The storage instance to remove.
        """
        storage_type = type(storage_instance)

        # Remove from ID cache
        type_cache_by_id = self._cache_by_id.get(storage_type, {})
        if storage_instance.id in type_cache_by_id:
            del type_cache_by_id[storage_instance.id]

        # Remove from name cache
        if storage_instance.name is not None:
            type_cache_by_name = self._cache_by_name.get(storage_type, {})
            if storage_instance.name in type_cache_by_name:
                del type_cache_by_name[storage_instance.name]

        # Remove from alias cache - need to search by instance since alias is not stored on the instance
        type_cache_by_alias = self._cache_by_alias.get(storage_type, {})
        aliases_to_remove = [alias for alias, instance in type_cache_by_alias.items() if instance is storage_instance]
        for alias in aliases_to_remove:
            del type_cache_by_alias[alias]

        # Remove from default instances
        if storage_type in self._default_instances and self._default_instances[storage_type] is storage_instance:
            del self._default_instances[storage_type]

    def clear_cache(self) -> None:
        """Clear all cached storage instances."""
        self._cache_by_id.clear()
        self._cache_by_name.clear()
        self._cache_by_alias.clear()
        self._default_instances.clear()
