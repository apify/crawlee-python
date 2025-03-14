from __future__ import annotations

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.storage_clients import StorageClient

from ._dataset_client import DatasetClient
from ._key_value_store_client import KeyValueStoreClient
from ._request_queue_client import RequestQueueClient


@docs_group('Classes')
class MemoryStorageClient(StorageClient):
    @override
    def dataset(self) -> type[DatasetClient]:
        return DatasetClient

    @override
    def key_value_store(self) -> type[KeyValueStoreClient]:
        return KeyValueStoreClient

    @override
    def request_queue(self) -> type[RequestQueueClient]:
        return RequestQueueClient
