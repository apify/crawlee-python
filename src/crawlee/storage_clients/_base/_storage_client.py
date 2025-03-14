from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._dataset_client import DatasetClient
    from ._key_value_store_client import KeyValueStoreClient
    from ._request_queue_client import RequestQueueClient


@dataclass
class StorageClient:
    dataset_client_class: type[DatasetClient]
    key_value_store_client_class: type[KeyValueStoreClient]
    request_queue_client_class: type[RequestQueueClient]
