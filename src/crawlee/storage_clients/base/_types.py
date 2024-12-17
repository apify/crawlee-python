from __future__ import annotations

from typing import Union

from ._base_dataset_client import BaseDatasetClient
from ._base_dataset_collection_client import BaseDatasetCollectionClient
from ._base_key_value_store_client import BaseKeyValueStoreClient
from ._base_key_value_store_collection_client import BaseKeyValueStoreCollectionClient
from ._base_request_queue_client import BaseRequestQueueClient
from ._base_request_queue_collection_client import BaseRequestQueueCollectionClient

ResourceClient = Union[
    BaseDatasetClient,
    BaseKeyValueStoreClient,
    BaseRequestQueueClient,
]

ResourceCollectionClient = Union[
    BaseDatasetCollectionClient,
    BaseKeyValueStoreCollectionClient,
    BaseRequestQueueCollectionClient,
]
