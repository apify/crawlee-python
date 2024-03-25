from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from crawlee.storages.request_queue import RequestQueue


@pytest.fixture()
async def request_queue() -> RequestQueue:
    return await RequestQueue.open()


async def test_open() -> None:
    default_request_queue = await RequestQueue.open()
    default_request_queue_by_id = await RequestQueue.open(id=default_request_queue._id)

    assert default_request_queue is default_request_queue_by_id

    request_queue_name = 'dummy-name'
    named_request_queue = await RequestQueue.open(name=request_queue_name)
    assert default_request_queue is not named_request_queue

    with pytest.raises(RuntimeError, match='Request queue with id "nonexistent-id" does not exist!'):
        await RequestQueue.open(id='nonexistent-id')

    # Test that when you try to open a request queue by ID and you use a name of an existing request queue,
    # it doesn't work
    with pytest.raises(RuntimeError, match='Request queue with id "dummy-name" does not exist!'):
        await RequestQueue.open(id='dummy-name')


async def test_same_references() -> None:
    rq1 = await RequestQueue.open()
    rq2 = await RequestQueue.open()
    assert rq1 is rq2

    rq_name = 'non-default'
    rq_named1 = await RequestQueue.open(name=rq_name)
    rq_named2 = await RequestQueue.open(name=rq_name)
    assert rq_named1 is rq_named2


async def test_drop() -> None:
    rq1 = await RequestQueue.open()
    await rq1.drop()
    rq2 = await RequestQueue.open()
    assert rq1 is not rq2


async def test_get_request(request_queue: RequestQueue) -> None:
    url = 'https://example.com'
    add_request_info = await request_queue.add_request(
        {
            'uniqueKey': url,
            'url': url,
        }
    )
    request = await request_queue.get_request(add_request_info['requestId'])
    assert request is not None
    assert request['url'] == url


async def test_add_fetch_handle_request(request_queue: RequestQueue) -> None:
    url = 'https://example.com'
    assert await request_queue.is_empty() is True
    with pytest.raises(ValueError, match='"url" is required'):
        await request_queue.add_request({})
    add_request_info = await request_queue.add_request(
        {
            'uniqueKey': url,
            'url': url,
        }
    )
    assert add_request_info['wasAlreadyPresent'] is False
    assert add_request_info['wasAlreadyHandled'] is False
    assert await request_queue.is_empty() is False

    # Fetch the request
    next_request = await request_queue.fetch_next_request()
    assert next_request is not None

    # Mark it as handled
    next_request['handledAt'] = datetime.now(timezone.utc)
    queue_operation_info = await request_queue.mark_request_as_handled(next_request)
    assert queue_operation_info is not None
    assert queue_operation_info['uniqueKey'] == url
    assert await request_queue.is_finished() is True


async def test_reclaim_request(request_queue: RequestQueue) -> None:
    url = 'https://example.com'
    await request_queue.add_request(
        {
            'uniqueKey': url,
            'url': url,
        }
    )
    # Fetch the request
    next_request = await request_queue.fetch_next_request()
    assert next_request is not None
    assert next_request['uniqueKey'] == url

    # Reclaim
    await request_queue.reclaim_request(next_request)
    # Try to fetch again after a few secs
    await asyncio.sleep(4)  # 3 seconds is the consistency delay in request queue
    next_again = await request_queue.fetch_next_request()
    assert next_again is not None
    assert next_again['uniqueKey'] == url
