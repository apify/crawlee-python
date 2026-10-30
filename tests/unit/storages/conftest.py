from __future__ import annotations

from importlib.util import find_spec
from typing import TYPE_CHECKING

import pytest

from crawlee import service_locator
from crawlee.storage_backends import (
    FileSystemStorageBackend,
    MemoryStorageBackend,
    RedisStorageBackend,
    SqlStorageBackend,
    StorageBackend,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fakeredis import FakeAsyncRedis


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip the Redis request queue cases when `lupa` is missing, since the Redis RQ backend runs Lua scripts."""
    if find_spec('lupa') is not None:
        return
    for item in items:
        callspec = getattr(item, 'callspec', None)
        if item.path.name == 'test_request_queue.py' and callspec and callspec.params.get('storage_backend') == 'redis':
            item.add_marker(pytest.mark.skip(reason='`lupa` is not installed'))


@pytest.fixture(params=['memory', 'file_system', 'sql', 'redis'])
async def storage_backend(
    request: pytest.FixtureRequest,
    redis_client: FakeAsyncRedis,
) -> AsyncGenerator[StorageBackend, None]:
    """Parameterized fixture to test with different storage backends."""
    storage_backend: StorageBackend

    storage_type = request.param

    if storage_type == 'memory':
        storage_backend = MemoryStorageBackend()
    elif storage_type == 'sql':
        storage_backend = SqlStorageBackend()
    elif storage_type == 'redis':
        storage_backend = RedisStorageBackend(redis=redis_client)
    else:
        storage_backend = FileSystemStorageBackend()
    service_locator.set_storage_backend(storage_backend)
    yield storage_backend

    if isinstance(storage_backend, SqlStorageBackend):
        await storage_backend.close()
