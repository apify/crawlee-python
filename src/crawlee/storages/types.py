from enum import Enum


class StorageTypes(str, Enum):
    """Possible Crawlee storage types."""

    DATASET = 'Dataset'
    KEY_VALUE_STORE = 'Key-value store'
    REQUEST_QUEUE = 'Request queue'
