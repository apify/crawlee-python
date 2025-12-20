from __future__ import annotations

from asyncio import Lock
from collections import defaultdict
from collections.abc import Coroutine, Hashable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar
from weakref import WeakValueDictionary

from crawlee._utils.raise_if_too_many_kwargs import raise_if_too_many_kwargs
from crawlee.storage_clients._base import DatasetClient, KeyValueStoreClient, RequestQueueClient

from ._utils import validate_storage_name

if TYPE_CHECKING:
    from ._base import Storage

T = TypeVar('T', bound='Storage')


@dataclass
class _StorageCache:
    """Cache for storage instances."""

    by_id: defaultdict[type[Storage], defaultdict[str, defaultdict[Hashable, Storage]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))
    )
    """Cache for storage instances by ID. Example: by_id[Dataset]['some_id']['some_additional_cache_key']."""

    by_name: defaultdict[type[Storage], defaultdict[str, defaultdict[Hashable, Storage]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))
    )
    """Cache for storage instances by name. Example: by_name[Dataset]['some_name']['some_additional_cache_key']"""

    by_alias: defaultdict[type[Storage], defaultdict[str, defaultdict[Hashable, Storage]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))
    )
    """Cache for storage instances by alias. Example: by_alias[Dataset]['some_alias']['some_additional_cache_key']"""

    def remove_from_cache(self, storage_instance: Storage) -> None:
        """Remove a storage instance from the cache.

        Args:
            storage_instance: The storage instance to remove.
        """
        storage_type = type(storage_instance)

        # Remove from ID cache
        for additional_key in self.by_id[storage_type][storage_instance.id]:
            del self.by_id[storage_type][storage_instance.id][additional_key]
            break

        # Remove from name cache or alias cache. It can never be in both.
        if storage_instance.name is not None:
            for additional_key in self.by_name[storage_type][storage_instance.name]:
                del self.by_name[storage_type][storage_instance.name][additional_key]
                break
        else:
            for alias_key in self.by_alias[storage_type]:
                for additional_key in self.by_alias[storage_type][alias_key]:
                    del self.by_alias[storage_type][alias_key][additional_key]
                    break


ClientOpenerCoro = Coroutine[None, None, DatasetClient | KeyValueStoreClient | RequestQueueClient]
"""Type alias for the client opener function."""


class StorageInstanceManager:
    """Manager for caching and managing storage instances.

    This class centralizes the caching logic for all storage types (Dataset, KeyValueStore, RequestQueue)
    and provides a unified interface for opening and managing storage instances.
    """

    _DEFAULT_STORAGE_ALIAS = '__default__'
    """Reserved alias for default unnamed storage."""

    def __init__(self) -> None:
        self._cache: _StorageCache = _StorageCache()
        self._opener_locks: WeakValueDictionary[tuple, Lock] = WeakValueDictionary()

    async def open_storage_instance(
        self,
        cls: type[T],
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        client_opener_coro: ClientOpenerCoro,
        storage_client_cache_key: Hashable = '',
    ) -> T:
        """Open a storage instance with caching support.

        Args:
            cls: The storage class to instantiate.
            id: Storage ID.
            name: Storage name. (global scope, persists across runs). Name can only contain letters "a" through "z",
                the digits "0" through "9", and the hyphen ("-") but only in the middle of the string
                (e.g. "my-value-1").
            alias: Storage alias (run scope, creates unnamed storage).
            client_opener_coro: Coroutine to open the storage client when storage instance not found in cache.
            storage_client_cache_key: Additional optional key from storage client to differentiate cache entries.

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
            raise_if_too_many_kwargs(id=id, name=name, alias=alias)

            # Auto-set alias='default' when no parameters are specified.
            # Default unnamed storage is equal to alias=default unnamed storage.
            if not any([name, alias, id]):
                alias = self._DEFAULT_STORAGE_ALIAS

            # Check cache without lock first for performance.
            if cached_instance := self._get_from_cache(
                cls,
                id=id,
                name=name,
                alias=alias,
                storage_client_cache_key=storage_client_cache_key,
            ):
                return cached_instance

            # Validate storage name
            if name is not None:
                validate_storage_name(name)

            # Acquire lock for this opener
            opener_lock_key = (cls, str(id or name or alias), storage_client_cache_key)
            if not (lock := self._opener_locks.get(opener_lock_key)):
                lock = Lock()
                self._opener_locks[opener_lock_key] = lock

            async with lock:
                # Another task could have created the storage while we were waiting for the lock - check if that
                # happened
                if cached_instance := self._get_from_cache(
                    cls,
                    id=id,
                    name=name,
                    alias=alias,
                    storage_client_cache_key=storage_client_cache_key,
                ):
                    return cached_instance

                # Check for conflicts between named and alias storages
                self._check_name_alias_conflict(
                    cls,
                    name=name,
                    alias=alias,
                    storage_client_cache_key=storage_client_cache_key,
                )

                # Create new instance
                client: KeyValueStoreClient | DatasetClient | RequestQueueClient
                client = await client_opener_coro

                metadata = await client.get_metadata()

                instance = cls(client, metadata.id, metadata.name)  # type: ignore[call-arg]
                instance_name = getattr(instance, 'name', None)

                # Cache the instance.
                # Note: No awaits in this section. All cache entries must be written
                # atomically to ensure pre-checks outside the lock see consistent state.

                # Always cache by id.
                self._cache.by_id[cls][instance.id][storage_client_cache_key] = instance

                # Cache named storage.
                if instance_name is not None:
                    self._cache.by_name[cls][instance_name][storage_client_cache_key] = instance

                # Cache unnamed storage.
                if alias is not None:
                    self._cache.by_alias[cls][alias][storage_client_cache_key] = instance

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
        self._cache.remove_from_cache(storage_instance)

    def clear_cache(self) -> None:
        """Clear all cached storage instances."""
        self._cache = _StorageCache()

    def _get_from_cache(
        self,
        cls: type[T],
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        storage_client_cache_key: Hashable = '',
    ) -> T | None:
        """Get a storage instance from the cache."""
        if id is not None and (cached_instance := self._cache.by_id[cls][id].get(storage_client_cache_key)):
            if isinstance(cached_instance, cls):
                return cached_instance
            raise RuntimeError('Cached instance type mismatch.')

        if name is not None and (cached_instance := self._cache.by_name[cls][name].get(storage_client_cache_key)):
            if isinstance(cached_instance, cls):
                return cached_instance
            raise RuntimeError('Cached instance type mismatch.')

        if alias is not None and (cached_instance := self._cache.by_alias[cls][alias].get(storage_client_cache_key)):
            if isinstance(cached_instance, cls):
                return cached_instance
            raise RuntimeError('Cached instance type mismatch.')

        return None

    def _check_name_alias_conflict(
        self,
        cls: type[T],
        *,
        name: str | None = None,
        alias: str | None = None,
        storage_client_cache_key: Hashable = '',
    ) -> None:
        """Check for conflicts between named and alias storages."""
        if alias and (self._cache.by_name[cls][alias].get(storage_client_cache_key)):
            raise ValueError(
                f'Cannot create alias storage "{alias}" because a named storage with the same name already exists. '
                f'Use a different alias or drop the existing named storage first.'
            )

        if name and (self._cache.by_alias[cls][name].get(storage_client_cache_key)):
            raise ValueError(
                f'Cannot create named storage "{name}" because an alias storage with the same name already exists. '
                f'Use a different name or drop the existing alias storage first.'
            )
