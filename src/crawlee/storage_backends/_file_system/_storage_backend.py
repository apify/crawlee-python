from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_backends._base import StorageBackend

from ._dataset_backend import FileSystemDatasetBackend
from ._key_value_store_backend import FileSystemKeyValueStoreBackend
from ._request_queue_backend import FileSystemRequestQueueBackend

if TYPE_CHECKING:
    from collections.abc import Hashable


@docs_group('Storage backends')
class FileSystemStorageBackend(StorageBackend):
    """File system implementation of the storage backend.

    This storage backend provides access to datasets, key-value stores, and request queues that persist data
    to the local file system. Each storage type is implemented with its own specific file system backend
    that stores data in a structured directory hierarchy.

    Data is stored in JSON format in predictable file paths, making it easy to inspect and manipulate
    the stored data outside of the Crawlee application if needed.

    All data persists between program runs but is limited to access from the local machine
    where the files are stored.

    Warning: This storage backend is not safe for concurrent access from multiple crawler processes.
    Use it only when running a single crawler process at a time.
    """

    @override
    def get_storage_backend_cache_key(self, configuration: Configuration) -> Hashable:
        # Even different backend instances should return same storage if the storage_dir is the same.
        return super().get_storage_backend_cache_key(configuration), configuration.storage_dir

    @override
    async def create_dataset_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> FileSystemDatasetBackend:
        configuration = configuration or Configuration.get_global_configuration()
        backend = await FileSystemDatasetBackend.open(id=id, name=name, alias=alias, configuration=configuration)
        await self._purge_if_needed(backend, configuration)
        return backend

    @override
    async def create_kvs_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> FileSystemKeyValueStoreBackend:
        configuration = configuration or Configuration.get_global_configuration()
        backend = await FileSystemKeyValueStoreBackend.open(id=id, name=name, alias=alias, configuration=configuration)
        await self._purge_if_needed(backend, configuration)
        return backend

    @override
    async def create_rq_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> FileSystemRequestQueueBackend:
        configuration = configuration or Configuration.get_global_configuration()
        backend = await FileSystemRequestQueueBackend.open(id=id, name=name, alias=alias, configuration=configuration)
        await self._purge_if_needed(backend, configuration)
        return backend
