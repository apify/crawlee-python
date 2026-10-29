from __future__ import annotations

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_backends._base import StorageBackend

from ._dataset_backend import MemoryDatasetBackend
from ._key_value_store_backend import MemoryKeyValueStoreBackend
from ._request_queue_backend import MemoryRequestQueueBackend


@docs_group('Storage backends')
class MemoryStorageBackend(StorageBackend):
    """Memory implementation of the storage backend.

    This storage backend provides access to datasets, key-value stores, and request queues that store all data
    in memory using Python data structures (lists and dictionaries). No data is persisted between process runs,
    meaning all stored data is lost when the program terminates.

    The memory implementation provides fast access to data but is limited by available memory and does not
    support data sharing across different processes. All storage operations happen entirely in memory with
    no disk operations.

    The memory storage backend is useful for testing and development environments, or short-lived crawler
    operations where persistence is not required.
    """

    @override
    async def create_dataset_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> MemoryDatasetBackend:
        configuration = configuration or Configuration.get_global_configuration()
        backend = await MemoryDatasetBackend.open(id=id, name=name, alias=alias)
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
    ) -> MemoryKeyValueStoreBackend:
        configuration = configuration or Configuration.get_global_configuration()
        backend = await MemoryKeyValueStoreBackend.open(id=id, name=name, alias=alias)
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
    ) -> MemoryRequestQueueBackend:
        configuration = configuration or Configuration.get_global_configuration()
        backend = await MemoryRequestQueueBackend.open(id=id, name=name, alias=alias)
        await self._purge_if_needed(backend, configuration)
        return backend
