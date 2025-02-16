from __future__ import annotations

from typing import Union

from ._dataset_client import DatasetClient
from ._dataset_collection_client import DatasetCollectionClient
from ._key_value_store_client import KeyValueStoreClient
from ._key_value_store_collection_client import KeyValueStoreCollectionClient
from ._request_queue_client import RequestQueueClient
from ._request_queue_collection_client import RequestQueueCollectionClient

ResourceClient = Union[
    DatasetClient,
    KeyValueStoreClient,
    RequestQueueClient,
]

ResourceCollectionClient = Union[
    DatasetCollectionClient,
    KeyValueStoreCollectionClient,
    RequestQueueCollectionClient,
]
