from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storage_clients._memory import RequestQueueCollectionClient


@pytest.fixture
def request_queues_client(memory_storage_client: MemoryStorageClient) -> RequestQueueCollectionClient:
    return memory_storage_client.request_queues()


async def test_get_or_create(request_queues_client: RequestQueueCollectionClient) -> None:
    rq_name = 'test'
    # A new request queue gets created
    rq_info = await request_queues_client.get_or_create(name=rq_name)
    assert rq_info.name == rq_name

    # Another get_or_create call returns the same request queue
    rq_existing = await request_queues_client.get_or_create(name=rq_name)
    assert rq_info.id == rq_existing.id
    assert rq_info.name == rq_existing.name
    assert rq_info.created_at == rq_existing.created_at


async def test_list(request_queues_client: RequestQueueCollectionClient) -> None:
    assert (await request_queues_client.list()).count == 0
    rq_info = await request_queues_client.get_or_create(name='dataset')
    rq_list = await request_queues_client.list()
    assert rq_list.count == 1
    assert rq_list.items[0].name == rq_info.name

    # Test sorting behavior
    newer_rq_info = await request_queues_client.get_or_create(name='newer-dataset')
    rq_list_sorting = await request_queues_client.list()
    assert rq_list_sorting.count == 2
    assert rq_list_sorting.items[0].name == rq_info.name
    assert rq_list_sorting.items[1].name == newer_rq_info.name
