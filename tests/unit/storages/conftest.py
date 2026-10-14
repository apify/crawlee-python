from __future__ import annotations

from importlib.util import find_spec
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
    from collections.abc import AsyncGenerator

    from fakeredis import FakeAsyncRedis


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip the Redis request queue cases when `lupa` is missing, since the Redis RQ client runs Lua scripts."""
    if find_spec('lupa') is not None:
        return
    for item in items:
        callspec = getattr(item, 'callspec', None)
        if item.path.name == 'test_request_queue.py' and callspec and callspec.params.get('storage_client') == 'redis':
            item.add_marker(pytest.mark.skip(reason='`lupa` is not installed'))


@pytest.fixture(params=['memory', 'file_system', 'sql', 'redis'])
async def storage_client(
    request: pytest.FixtureRequest,
    redis_client: FakeAsyncRedis,
) -> AsyncGenerator[StorageClient, None]:
    """Parameterized fixture to test with different storage clients."""
    storage_client: StorageClient

    storage_type = request.param

    if storage_type == 'memory':
        storage_client = MemoryStorageClient()
    elif storage_type == 'sql':
        storage_client = SqlStorageClient()
    elif storage_type == 'redis':
        storage_client = RedisStorageClient(redis=redis_client)
    else:
        storage_client = FileSystemStorageClient()
    service_locator.set_storage_client(storage_client)
    yield storage_client

    if isinstance(storage_client, SqlStorageClient):
        await storage_client.close()
