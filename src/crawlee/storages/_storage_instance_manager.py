from __future__ import annotations

from collections import defaultdict
from collections.abc import Coroutine, Hashable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar, cast

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
    """Cache for storage instances by ID, separated by storage type."""
    by_name: defaultdict[type[Storage], defaultdict[str, defaultdict[Hashable, Storage]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))
    )
    """Cache for storage instances by name, separated by storage type."""
    default_instances: defaultdict[type[Storage], defaultdict[Hashable, Storage]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict())
    )
    """Cache for default instances of each storage type."""


StorageClientType = DatasetClient | KeyValueStoreClient | RequestQueueClient
"""Type alias for the storage client types."""

ClientOpener = Coroutine[None, None, StorageClientType]
"""Type alias for the client opener function."""


class StorageInstanceManager:
    """Manager for caching and managing storage instances.

    This class centralizes the caching logic for all storage types (Dataset, KeyValueStore, RequestQueue)
    and provides a unified interface for opening and managing storage instances.
    """

    def __init__(self) -> None:
        self._cache_by_storage_client: dict[type[StorageClient], _StorageClientCache] = defaultdict(_StorageClientCache)

    async def open_storage_instance(
        self,
        cls: type[T],
        *,
        id: str | None,
        name: str | None,
        storage_client_type: type[StorageClient],
        client_opener: ClientOpener,
        additional_cache_key: Hashable = '',
    ) -> T:
        """Open a storage instance with caching support.

        Args:
            cls: The storage class to instantiate.
            id: Storage ID.
            name: Storage name.
            storage_client_type: Type of storage client to use.
            client_opener: Coroutine to open the storage client when storage instance not found in cache.
            additional_cache_key: Additional optional key to differentiate cache entries.

        Returns:
            The storage instance.

        Raises:
            ValueError: If both id and name are specified.
        """
        if id and name:
            raise ValueError('Only one of "id" or "name" can be specified, not both.')

        # Check for default instance
        if (
            id is None
            and name is None
            and additional_cache_key in self._cache_by_storage_client[storage_client_type].default_instances[cls]
        ):
            client_opener.close()  # Close the opener since we don't need it
            return cast(
                'T', self._cache_by_storage_client[storage_client_type].default_instances[cls][additional_cache_key]
            )

        # Check cache
        if id is not None and (
            cached_instance := self._cache_by_storage_client[storage_client_type]
            .by_id[cls][id]
            .get(additional_cache_key)
        ):
            if isinstance(cached_instance, cls):
                client_opener.close()  # Close the opener since we don't need it
                return cached_instance
            raise RuntimeError('Cached instance type mismatch.')

        if name is not None and (
            cached_instance := self._cache_by_storage_client[storage_client_type]
            .by_name[cls][name]
            .get(additional_cache_key)
        ):
            if isinstance(cached_instance, cls):
                client_opener.close()  # Close the opener since we don't need it
                return cached_instance
            raise RuntimeError('Cached instance type mismatch.')

        client: KeyValueStoreClient | DatasetClient | RequestQueueClient
        # Create new instance
        client = await client_opener

        metadata = await client.get_metadata()

        instance = cls(client, metadata.id, metadata.name)  # type: ignore[call-arg]
        instance_name = getattr(instance, 'name', None)

        # Cache the instance
        self._cache_by_storage_client[storage_client_type].by_id[cls][instance.id][additional_cache_key] = instance
        if instance_name is not None:
            self._cache_by_storage_client[storage_client_type].by_name[cls][instance_name][additional_cache_key] = (
                instance
            )

        # Set as default if no id/name specified
        if id is None and name is None:
            self._cache_by_storage_client[storage_client_type].default_instances[cls][additional_cache_key] = instance

        return instance

    def remove_from_cache(self, storage_instance: Storage) -> None:
        """Remove a storage instance from the cache.

        Args:
            storage_instance: The storage instance to remove.
        """
        storage_type = type(storage_instance)

        for storage_client_cache in self._cache_by_storage_client.values():
            # Remove from ID cache
            for additional_key in storage_client_cache.by_id[storage_type][storage_instance.id]:
                if additional_key in storage_client_cache.by_id[storage_type][storage_instance.id]:
                    del storage_client_cache.by_id[storage_type][storage_instance.id][additional_key]
                    break

            # Remove from name cache
            if storage_instance.name is not None:
                for additional_key in storage_client_cache.by_name[storage_type][storage_instance.name]:
                    if additional_key in storage_client_cache.by_name[storage_type][storage_instance.name]:
                        del storage_client_cache.by_name[storage_type][storage_instance.name][additional_key]
                        break

        # Remove from default instances
        for additional_key in storage_client_cache.default_instances[storage_type]:
            if storage_client_cache.default_instances[storage_type][additional_key] is storage_instance:
                del storage_client_cache.default_instances[storage_type][additional_key]
                break

    def clear_cache(self) -> None:
        """Clear all cached storage instances."""
        self._cache_by_storage_client = defaultdict(_StorageClientCache)
