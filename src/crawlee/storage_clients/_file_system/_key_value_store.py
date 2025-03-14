from __future__ import annotations

from logging import getLogger

from crawlee.storage_clients._base import KeyValueStoreClient

logger = getLogger(__name__)


class FileSystemKeyValueStoreClient(KeyValueStoreClient):
    pass
