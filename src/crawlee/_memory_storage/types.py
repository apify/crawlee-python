from enum import Enum


class Storage(str, Enum):
    """Enum of all possible storage types."""

    DATASET = 'Dataset'
    KEY_VALUE_STORE = 'Key-value store'
    REQUEST_QUEUE = 'Request queue'
