from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest
from redis.asyncio import Redis

from crawlee.configuration import Configuration
from crawlee.storage_backends import RedisStorageBackend
from crawlee.storages import Dataset

if TYPE_CHECKING:
    from fakeredis import FakeAsyncRedis


def test_import_error_handled() -> None:
    blocked = {mod_name: None for mod_name in sys.modules if mod_name == 'redis' or mod_name.startswith('redis.')}
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith('crawlee.storage_backends._redis'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.storage_backends._redis import RedisStorageBackend  # noqa: F401 PLC0415


async def test_close_closes_own_client(suppress_user_warning: None) -> None:  # noqa: ARG001
    """A client created from the connection string is closed with the backend, and only once."""
    with patch.object(Redis, 'aclose', AsyncMock()) as aclose:
        async with RedisStorageBackend(connection_string='redis://localhost:6379') as storage_backend:
            pass

        await storage_backend.close()

    aclose.assert_awaited_once()


async def test_close_keeps_passed_client(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """A client passed in belongs to the caller, so the backend doesn't close it."""
    with patch.object(redis_client, 'aclose', AsyncMock()) as aclose:
        async with RedisStorageBackend(redis=redis_client):
            pass

    aclose.assert_not_awaited()


async def test_closed_backend_raises(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """A closed backend refuses to open storages and to serve the ones opened before."""
    configuration = Configuration(purge_on_start=False)

    async with RedisStorageBackend(redis=redis_client) as storage_backend:
        kvs_backend = await storage_backend.create_kvs_backend(configuration=configuration)

    with pytest.raises(RuntimeError, match=r'RedisStorageBackend is closed'):
        await kvs_backend.set_value(key='key', value=b'value', content_type='text/plain')

    with pytest.raises(RuntimeError, match=r'RedisStorageBackend is closed'):
        await storage_backend.create_kvs_backend(configuration=configuration)


async def test_new_backend_skips_storages_of_closed_one(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """A new backend opens its own storage instead of the cached one bound to a closed backend."""
    configuration = Configuration(purge_on_start=False)

    async with RedisStorageBackend(redis=redis_client) as storage_backend:
        old_dataset = await Dataset.open(name='results', storage_backend=storage_backend, configuration=configuration)

    async with RedisStorageBackend(redis=redis_client) as storage_backend:
        dataset = await Dataset.open(name='results', storage_backend=storage_backend, configuration=configuration)
        await dataset.push_data({'key': 'value'})

    assert dataset is not old_dataset


async def test_new_backend_purges_default_dataset(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """A new backend with `purge_on_start` enabled purges the default dataset an earlier backend wrote."""
    configuration = Configuration(purge_on_start=True)

    async with RedisStorageBackend(redis=redis_client) as storage_backend:
        dataset = await Dataset.open(storage_backend=storage_backend, configuration=configuration)
        await dataset.push_data({'key': 'value'})

    async with RedisStorageBackend(redis=redis_client) as storage_backend:
        dataset = await Dataset.open(storage_backend=storage_backend, configuration=configuration)
        data = await dataset.get_data()

    assert data.items == []
