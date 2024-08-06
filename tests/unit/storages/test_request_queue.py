from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from crawlee.models import BaseRequestData, Request
from crawlee.storages import RequestQueue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence


@pytest.fixture()
async def request_queue() -> AsyncGenerator[RequestQueue, None]:
    rq = await RequestQueue.open()
    yield rq
    await rq.drop()


async def test_open() -> None:
    default_request_queue = await RequestQueue.open()
    default_request_queue_by_id = await RequestQueue.open(id=default_request_queue.id)

    assert default_request_queue is default_request_queue_by_id

    request_queue_name = 'dummy-name'
    named_request_queue = await RequestQueue.open(name=request_queue_name)
    assert default_request_queue is not named_request_queue

    with pytest.raises(RuntimeError, match='RequestQueue with id "nonexistent-id" does not exist!'):
        await RequestQueue.open(id='nonexistent-id')

    # Test that when you try to open a request queue by ID and you use a name of an existing request queue,
    # it doesn't work
    with pytest.raises(RuntimeError, match='RequestQueue with id "dummy-name" does not exist!'):
        await RequestQueue.open(id='dummy-name')


async def test_consistency_accross_two_clients() -> None:
    request_apify = Request.from_url('https://apify.com')
    request_crawlee = Request.from_url('https://crawlee.dev')

    rq = await RequestQueue.open(name='my-rq')
    await rq.add_request(request_apify)

    rq_by_id = await RequestQueue.open(id=rq.id)
    await rq_by_id.add_request(request_crawlee)

    assert await rq.get_total_count() == 2
    assert await rq_by_id.get_total_count() == 2

    assert await rq.fetch_next_request() == request_apify
    assert await rq_by_id.fetch_next_request() == request_crawlee

    await rq.drop()
    with pytest.raises(RuntimeError, match='Storage with provided ID was not found'):
        await rq_by_id.drop()


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
    request = Request.from_url('https://example.com')
    processed_request = await request_queue.add_request(request)
    assert request.id == processed_request.id
    request_2 = await request_queue.get_request(request.id)
    assert request_2 is not None
    assert request == request_2


async def test_add_fetch_handle_request(request_queue: RequestQueue) -> None:
    request = Request.from_url('https://example.com')
    assert await request_queue.is_empty() is True
    add_request_info = await request_queue.add_request(request)

    assert add_request_info.was_already_present is False
    assert add_request_info.was_already_handled is False
    assert await request_queue.is_empty() is False

    # Fetch the request
    next_request = await request_queue.fetch_next_request()
    assert next_request is not None

    # Mark it as handled
    next_request.handled_at = datetime.now(timezone.utc)
    processed_request = await request_queue.mark_request_as_handled(next_request)

    assert processed_request is not None
    assert processed_request.id == request.id
    assert processed_request.unique_key == request.unique_key
    assert await request_queue.is_finished() is True


async def test_reclaim_request(request_queue: RequestQueue) -> None:
    request = Request.from_url('https://example.com')
    await request_queue.add_request(request)

    # Fetch the request
    next_request = await request_queue.fetch_next_request()
    assert next_request is not None
    assert next_request.unique_key == request.url

    # Reclaim
    await request_queue.reclaim_request(next_request)
    # Try to fetch again after a few secs
    await asyncio.sleep(4)  # 3 seconds is the consistency delay in request queue
    next_again = await request_queue.fetch_next_request()

    assert next_again is not None
    assert next_again.id == request.id
    assert next_again.unique_key == request.unique_key


@pytest.mark.parametrize(
    'requests',
    [
        [BaseRequestData.from_url('https://example.com')],
        [Request.from_url('https://apify.com')],
        ['https://crawlee.dev'],
        [Request.from_url(f'https://example.com/{i}') for i in range(10)],
        [f'https://example.com/{i}' for i in range(15)],
    ],
    ids=['single-base-request', 'single-request', 'single-url', 'multiple-requests', 'multiple-urls'],
)
async def test_add_batched_requests(
    request_queue: RequestQueue,
    requests: Sequence[str | BaseRequestData | Request],
) -> None:
    request_count = len(requests)

    # Add the requests to the RQ in batches
    await request_queue.add_requests_batched(requests, wait_for_all_requests_to_be_added=True)

    # Ensure the batch was processed correctly
    assert await request_queue.get_total_count() == request_count

    # Fetch and validate each request in the queue
    for original_request in requests:
        next_request = await request_queue.fetch_next_request()
        assert next_request is not None

        expected_url = original_request if isinstance(original_request, str) else original_request.url
        assert next_request.url == expected_url

    # Confirm the queue is empty after processing all requests
    assert await request_queue.is_empty() is True
