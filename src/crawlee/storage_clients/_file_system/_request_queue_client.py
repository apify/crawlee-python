from __future__ import annotations

from logging import getLogger

from crawlee.storage_clients._base import RequestQueueClient

logger = getLogger(__name__)


class FileSystemRequestQueueClient(RequestQueueClient):
    pass
