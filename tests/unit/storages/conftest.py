from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from crawlee import service_locator
from crawlee.storage_clients import (
    FileSystemStorageClient,
    MemoryStorageClient,
    RedisStorageClient,
    SqlStorageClient,
    StorageClient,
)

if TYPE_CHECKING:
    from fakeredis import FakeAsyncRedis


@pytest.fixture(params=['memory', 'file_system', 'sql', ('redis', 'default'), ('redis', 'bloom')])
def storage_client(
    request: pytest.FixtureRequest,
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> StorageClient:
    """Parameterized fixture to test with different storage clients."""
    storage_client: StorageClient

    if isinstance(request.param, tuple):
        storage_type, storage_config = request.param
    else:
        storage_type = request.param
        storage_config = None

    if storage_type == 'memory':
        storage_client = MemoryStorageClient()
    elif storage_type == 'sql':
        storage_client = SqlStorageClient()
    elif storage_type == 'redis' and storage_config in ('default', 'bloom'):
        storage_client = RedisStorageClient(redis=redis_client, queue_dedup_strategy=storage_config)
    else:
        storage_client = FileSystemStorageClient()
    service_locator.set_storage_client(storage_client)
    return storage_client
