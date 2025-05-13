from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from itertools import count
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from crawlee import Request, service_locator
from crawlee._request import RequestState
from crawlee.storage_clients import MemoryStorageClient, StorageClient
from crawlee.storage_clients._memory import RequestQueueClient
from crawlee.storage_clients.models import (
    BatchRequestsOperationResponse,
    StorageMetadata,
    UnprocessedRequest,
)
from crawlee.storages import RequestQueue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence


@pytest.fixture
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
        [Request.from_url('https://apify.com')],
        ['https://crawlee.dev'],
        [Request.from_url(f'https://example.com/{i}') for i in range(10)],
        [f'https://example.com/{i}' for i in range(15)],
    ],
    ids=['single-request', 'single-url', 'multiple-requests', 'multiple-urls'],
)
async def test_add_batched_requests(
    request_queue: RequestQueue,
    requests: Sequence[str | Request],
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


async def test_invalid_user_data_serialization() -> None:
    with pytest.raises(ValidationError):
        Request.from_url(
            'https://crawlee.dev',
            user_data={
                'foo': datetime(year=2020, month=7, day=4, tzinfo=timezone.utc),
                'bar': {datetime(year=2020, month=4, day=7, tzinfo=timezone.utc)},
            },
        )


async def test_user_data_serialization(request_queue: RequestQueue) -> None:
    request = Request.from_url(
        'https://crawlee.dev',
        user_data={
            'hello': 'world',
            'foo': 42,
        },
    )

    await request_queue.add_request(request)

    dequeued_request = await request_queue.fetch_next_request()
    assert dequeued_request is not None

    assert dequeued_request.user_data['hello'] == 'world'
    assert dequeued_request.user_data['foo'] == 42


async def test_complex_user_data_serialization(request_queue: RequestQueue) -> None:
    request = Request.from_url('https://crawlee.dev')
    request.user_data['hello'] = 'world'
    request.user_data['foo'] = 42
    request.crawlee_data.max_retries = 1
    request.crawlee_data.state = RequestState.ERROR_HANDLER

    await request_queue.add_request(request)

    dequeued_request = await request_queue.fetch_next_request()
    assert dequeued_request is not None

    data = dequeued_request.model_dump(by_alias=True)
    assert data['userData']['hello'] == 'world'
    assert data['userData']['foo'] == 42
    assert data['userData']['__crawlee'] == {
        'maxRetries': 1,
        'state': RequestState.ERROR_HANDLER,
    }


async def test_deduplication_of_requests_with_custom_unique_key() -> None:
    with pytest.raises(ValueError, match='`always_enqueue` cannot be used with a custom `unique_key`'):
        Request.from_url('https://apify.com', unique_key='apify', always_enqueue=True)


async def test_deduplication_of_requests_with_invalid_custom_unique_key() -> None:
    request_1 = Request.from_url('https://apify.com', always_enqueue=True)
    request_2 = Request.from_url('https://apify.com', always_enqueue=True)

    rq = await RequestQueue.open(name='my-rq')
    await rq.add_request(request_1)
    await rq.add_request(request_2)

    assert await rq.get_total_count() == 2

    assert await rq.fetch_next_request() == request_1
    assert await rq.fetch_next_request() == request_2


async def test_deduplication_of_requests_with_valid_custom_unique_key() -> None:
    request_1 = Request.from_url('https://apify.com')
    request_2 = Request.from_url('https://apify.com')

    rq = await RequestQueue.open(name='my-rq')
    await rq.add_request(request_1)
    await rq.add_request(request_2)

    assert await rq.get_total_count() == 1

    assert await rq.fetch_next_request() == request_1


async def test_cache_requests(request_queue: RequestQueue) -> None:
    request_1 = Request.from_url('https://apify.com')
    request_2 = Request.from_url('https://crawlee.dev')

    await request_queue.add_request(request_1)
    await request_queue.add_request(request_2)

    assert request_queue._requests_cache.currsize == 2

    fetched_request = await request_queue.fetch_next_request()

    assert fetched_request is not None
    assert fetched_request.id == request_1.id

    # After calling fetch_next_request request_1 moved to the end of the cache store.
    cached_items = [request_queue._requests_cache.popitem()[0] for _ in range(2)]
    assert cached_items == [request_2.id, request_1.id]


async def test_from_storage_object() -> None:
    storage_client = service_locator.get_storage_client()

    storage_object = StorageMetadata(
        id='dummy-id',
        name='dummy-name',
        accessed_at=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
        modified_at=datetime.now(timezone.utc),
        extra_attribute='extra',
    )

    request_queue = RequestQueue.from_storage_object(storage_client, storage_object)

    assert request_queue.id == storage_object.id
    assert request_queue.name == storage_object.name
    assert request_queue.storage_object == storage_object
    assert storage_object.model_extra.get('extra_attribute') == 'extra'  # type: ignore[union-attr]


async def test_add_batched_requests_with_retry(request_queue: RequestQueue) -> None:
    """Test that unprocessed requests are retried.

    Unprocessed requests should not count in `get_total_count`
    Test creates situation where in `batch_add_requests` call in first batch 3 requests are unprocessed.
    On each following `batch_add_requests` call the last request in batch remains unprocessed.
    In this test `batch_add_requests` is called once with batch of 10 requests. With retries only 1 request should
    remain unprocessed."""

    batch_add_requests_call_counter = count(start=1)
    service_locator.get_storage_client()
    initial_request_count = 10
    expected_added_requests = 9
    requests = [f'https://example.com/{i}' for i in range(initial_request_count)]

    class MockedRequestQueueClient(RequestQueueClient):
        """Patched memory storage client that simulates unprocessed requests."""

        async def _batch_add_requests_without_last_n(
            self, batch: Sequence[Request], n: int = 0
        ) -> BatchRequestsOperationResponse:
            response = await super().batch_add_requests(batch[:-n])
            response.unprocessed_requests = [
                UnprocessedRequest(url=r.url, unique_key=r.unique_key, method=r.method) for r in batch[-n:]
            ]
            return response

        async def batch_add_requests(
            self,
            requests: Sequence[Request],
            *,
            forefront: bool = False,  # noqa: ARG002
        ) -> BatchRequestsOperationResponse:
            """Mocked client behavior that simulates unprocessed requests.

            It processes all except last three at first run, then all except last none.
            Overall if tried with the same batch it will process all except the last one.
            """
            call_count = next(batch_add_requests_call_counter)
            if call_count == 1:
                # Process all but last three
                return await self._batch_add_requests_without_last_n(requests, n=3)
            # Process all but last
            return await self._batch_add_requests_without_last_n(requests, n=1)

    mocked_storage_client = AsyncMock(spec=StorageClient)
    mocked_storage_client.request_queue = MagicMock(
        return_value=MockedRequestQueueClient(id='default', memory_storage_client=MemoryStorageClient.from_config())
    )

    request_queue = RequestQueue(id='default', name='some_name', storage_client=mocked_storage_client)

    # Add the requests to the RQ in batches
    await request_queue.add_requests_batched(
        requests, wait_for_all_requests_to_be_added=True, wait_time_between_batches=timedelta(0)
    )

    # Ensure the batch was processed correctly
    assert await request_queue.get_total_count() == expected_added_requests
    # Fetch and validate each request in the queue
    for original_request in requests[:expected_added_requests]:
        next_request = await request_queue.fetch_next_request()
        assert next_request is not None

        expected_url = original_request if isinstance(original_request, str) else original_request.url
        assert next_request.url == expected_url

    # Confirm the queue is empty after processing all requests
    assert await request_queue.is_empty() is True
