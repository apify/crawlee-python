from __future__ import annotations

from crawlee.storage_clients._base import StorageClient

from ._dataset_client import MemoryDatasetClient
from ._key_value_store_client import MemoryKeyValueStoreClient
from ._request_queue_client import MemoryRequestQueueClient

memory_storage_client = StorageClient(
    dataset_client_class=MemoryDatasetClient,
    key_value_store_client_class=MemoryKeyValueStoreClient,
    request_queue_client_class=MemoryRequestQueueClient,
)
