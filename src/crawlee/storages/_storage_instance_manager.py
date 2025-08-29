from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, cast

from mypy_extensions import DefaultNamedArg

from crawlee.storage_clients._base import DatasetClient, KeyValueStoreClient, RequestQueueClient

from . import Dataset, KeyValueStore, RequestQueue

if TYPE_CHECKING:
    from crawlee.storage_clients import StorageClient

    from ._base import Storage

T = TypeVar('T', bound='Storage')

ClientOpener = Callable[
    [DefaultNamedArg(str | None, 'id'), DefaultNamedArg(str | None, 'name')],
    Coroutine[Any, Any, DatasetClient | KeyValueStoreClient | RequestQueueClient],
]
"""Type alias for the client opener function."""


@dataclass
class _StorageClientCache:
    """Cache for specific storage client."""

    by_id: defaultdict[type[Storage], defaultdict[str, Storage]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict())
    )
    """Cache for storage instances by ID, separated by storage type."""
    by_name: defaultdict[type[Storage], defaultdict[str, Storage]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict())
    )
    """Cache for storage instances by name, separated by storage type."""
    default_instances: defaultdict[type[Storage], Storage] = field(default_factory=lambda: defaultdict())
    """Cache for default instances of each storage type."""


class StorageInstanceManager:
    """Manager for caching and managing storage instances.

    This class centralizes the caching logic for all storage types (Dataset, KeyValueStore, RequestQueue)
    and provides a unified interface for opening and managing storage instances.
    """

    def __init__(self) -> None:
        self._cache_by_storage_client: dict[StorageClient, _StorageClientCache] = defaultdict(_StorageClientCache)

    async def open_storage_instance(
        self,
        cls: type[T],
        *,
        id: str | None,
        name: str | None,
        storage_client: StorageClient,
    ) -> T:
        """Open a storage instance with caching support.

        Args:
            cls: The storage class to instantiate.
            id: Storage ID.
            name: Storage name.
            storage_client: Storage client instance.

        Returns:
            The storage instance.

        Raises:
            ValueError: If both id and name are specified.
        """
        if id and name:
            raise ValueError('Only one of "id" or "name" can be specified, not both.')

        # Check for default instance
        if id is None and name is None and cls in self._cache_by_storage_client[storage_client].default_instances:
            return cast('T', self._cache_by_storage_client[storage_client].default_instances[cls])

        # Check cache
        if id is not None:
            type_cache_by_id = self._cache_by_storage_client[storage_client].by_id[cls]
            if id in type_cache_by_id:
                cached_instance = type_cache_by_id[id]
                if isinstance(cached_instance, cls):
                    return cached_instance

        if name is not None:
            type_cache_by_name = self._cache_by_storage_client[storage_client].by_name[cls]
            if name in type_cache_by_name:
                cached_instance = type_cache_by_name[name]
                if isinstance(cached_instance, cls):
                    return cached_instance

        client_opener: ClientOpener
        # Create new instance
        if cls is Dataset:
            client_opener = storage_client.create_dataset_client
        elif cls is KeyValueStore:
            client_opener = storage_client.create_kvs_client
        elif cls is RequestQueue:
            client_opener = storage_client.create_rq_client
        else:
            raise ValueError(f'Unsupported storage class: {cls.__name__}')

        client = await client_opener(id=id, name=name)
        metadata = await client.get_metadata()

        instance = cls(client, metadata.id, metadata.name)  # type: ignore[call-arg]
        instance_name = getattr(instance, 'name', None)

        # Cache the instance
        self._cache_by_storage_client[storage_client].by_id[cls][instance.id] = instance
        if instance_name is not None:
            self._cache_by_storage_client[storage_client].by_name[cls][instance_name] = instance

        # Set as default if no id/name specified
        if id is None and name is None:
            self._cache_by_storage_client[storage_client].default_instances[cls] = instance

        return instance

    def remove_from_cache(self, storage_instance: Storage) -> None:
        """Remove a storage instance from the cache.

        Args:
            storage_instance: The storage instance to remove.
        """
        storage_type = type(storage_instance)

        # Remove from ID cache
        for client_cache in self._cache_by_storage_client.values():
            type_cache_by_id = client_cache.by_id[storage_type]
            if storage_instance.id in type_cache_by_id:
                del type_cache_by_id[storage_instance.id]

            # Remove from name cache
            type_cache_by_name = client_cache.by_name[storage_type]
            if storage_instance.name in type_cache_by_name and storage_instance.name:
                del type_cache_by_name[storage_instance.name]

            # Remove from default instances
            if (
                storage_type in client_cache.default_instances
                and client_cache.default_instances[storage_type] is storage_instance
            ):
                del client_cache.default_instances[storage_type]

    def clear_cache(self) -> None:
        """Clear all cached storage instances."""
        self._cache_by_storage_client = defaultdict(_StorageClientCache)
