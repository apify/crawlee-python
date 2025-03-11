from __future__ import annotations

from typing import Union

from ._dataset_client import DatasetClient
from ._key_value_store_client import KeyValueStoreClient
from ._request_queue_client import RequestQueueClient

ResourceClient = Union[
    DatasetClient,
    KeyValueStoreClient,
    RequestQueueClient,
]
