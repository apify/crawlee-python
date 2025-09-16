from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from crawlee import service_locator
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient, RedisStorageClient, StorageClient

if TYPE_CHECKING:
    from fakeredis import FakeAsyncRedis


@pytest.fixture(params=['memory', 'file_system', 'redis'])
def storage_client(
    request: pytest.FixtureRequest,
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> StorageClient:
    """Parameterized fixture to test with different storage clients."""
    storage_client: StorageClient
    if request.param == 'memory':
        storage_client = MemoryStorageClient()
    elif request.param == 'redis':
        storage_client = RedisStorageClient(redis=redis_client)
    else:
        storage_client = FileSystemStorageClient()
    service_locator.set_storage_client(storage_client)
    return storage_client
