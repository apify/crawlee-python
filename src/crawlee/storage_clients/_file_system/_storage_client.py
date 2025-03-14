from __future__ import annotations

from crawlee.storage_clients._base import StorageClient

from ._dataset_client import FileSystemDatasetClient
from ._key_value_store import FileSystemKeyValueStoreClient
from ._request_queue import FileSystemRequestQueueClient

file_system_storage_client = StorageClient(
    dataset_client_class=FileSystemDatasetClient,
    key_value_store_client_class=FileSystemKeyValueStoreClient,
    request_queue_client_class=FileSystemRequestQueueClient,
)
