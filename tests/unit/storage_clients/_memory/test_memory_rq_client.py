from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.storage_clients import MemoryStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_clients._memory import MemoryRequestQueueClient


@pytest.fixture
async def rq_client() -> AsyncGenerator[MemoryRequestQueueClient, None]:
    """Fixture that provides a fresh memory request queue client for each test."""
    client = await MemoryStorageClient().create_rq_client(name='test-rq')
    yield client
    await client.drop()


async def test_memory_specific_purge_behavior() -> None:
    """Test memory-specific purge behavior and in-memory storage characteristics."""
    # Create RQ and add data
    rq_client1 = await MemoryStorageClient().create_rq_client(
        name='test-purge-rq',
    )
    request = Request.from_url(url='https://example.com/initial')
    await rq_client1.add_batch_of_requests([request])

    # Verify request was added
    assert await rq_client1.is_empty() is False

    # Reopen with same storage client instance
    rq_client2 = await MemoryStorageClient().create_rq_client(
        name='test-purge-rq',
    )

    # Verify queue was purged (memory storage specific behavior)
    assert await rq_client2.is_empty() is True


async def test_add_existing_pending_request_returns_single_processed_request(
    rq_client: MemoryRequestQueueClient,
) -> None:
    """Test that re-adding a pending (not handled) request yields exactly one `ProcessedRequest` entry."""
    request = Request.from_url('https://example.com')
    await rq_client.add_batch_of_requests([request])

    # Re-add the same request while it is still pending (not handled, not in progress).
    response = await rq_client.add_batch_of_requests([request])

    assert len(response.processed_requests) == 1
    processed_request = response.processed_requests[0]
    assert processed_request.unique_key == request.unique_key
    assert processed_request.was_already_present is True
    assert processed_request.was_already_handled is False


async def test_memory_metadata_updates(rq_client: MemoryRequestQueueClient) -> None:
    """Test that metadata timestamps are updated correctly in memory storage."""
    # Record initial timestamps
    metadata = await rq_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await rq_client.is_empty()

    # Verify timestamps (memory-specific behavior)
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify timestamps were updated
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read


async def test_forefront_readd_does_not_grow_pending_requests(rq_client: MemoryRequestQueueClient) -> None:
    """Test that repeatedly repositioning pending requests does not create duplicate entries."""
    requests = [Request.from_url(f'https://example.com/{i}') for i in range(20)]
    await rq_client.add_batch_of_requests(requests)

    for _ in range(10):
        await rq_client.add_batch_of_requests(requests, forefront=True)

    assert len(rq_client._pending_requests) == len(requests)


async def test_forefront_readd_preserves_order_and_dedup(rq_client: MemoryRequestQueueClient) -> None:
    """Test that repositioning already-pending requests to the forefront keeps LIFO order and dedup."""
    requests = [Request.from_url(f'https://example.com/{i}') for i in range(3)]
    await rq_client.add_batch_of_requests(requests)

    # Re-add a subset (0 and 1) to the forefront while still pending. Request 1 is added last, so it must
    # end up at the very front, followed by request 0, then the untouched regular request 2.
    await rq_client.add_batch_of_requests(requests[:2], forefront=True)

    fetched_urls = []
    while (request := await rq_client.fetch_next_request()) is not None:
        fetched_urls.append(request.url)
        await rq_client.mark_request_as_handled(request)

    assert fetched_urls == [
        'https://example.com/1',
        'https://example.com/0',
        'https://example.com/2',
    ]

    # No stale duplicates should linger after all live requests are drained.
    assert await rq_client.is_empty() is True
    assert await rq_client.is_finished() is True


async def test_regular_readd_of_pending_request_is_not_dropped(rq_client: MemoryRequestQueueClient) -> None:
    """Test that a regular re-add updates a still-pending request without dropping it."""
    original = Request.from_url('https://example.com/page')
    await rq_client.add_batch_of_requests([original])

    # Re-add the same URL while still pending, as a distinct object (as the higher-level API does when it
    # rebuilds requests). `forefront` defaults to False.
    duplicate = Request.from_url('https://example.com/page')
    duplicate.user_data['version'] = 2
    assert duplicate is not original
    assert duplicate.unique_key == original.unique_key
    await rq_client.add_batch_of_requests([duplicate])

    # The request must still be pending and fetchable exactly once, and the counts must stay consistent.
    assert await rq_client.is_empty() is False

    fetched = await rq_client.fetch_next_request()
    assert fetched is duplicate
    assert fetched.user_data['version'] == 2
    await rq_client.mark_request_as_handled(fetched)

    assert await rq_client.fetch_next_request() is None
    assert await rq_client.is_empty() is True
    assert await rq_client.is_finished() is True

    metadata = await rq_client.get_metadata()
    assert metadata.total_request_count == 1
    assert metadata.pending_request_count == 0
    assert metadata.handled_request_count == 1


async def test_regular_readd_does_not_reorder_pending_queue(rq_client: MemoryRequestQueueClient) -> None:
    """Test that a regular re-add of an already-pending request leaves the FIFO order untouched."""
    requests = [Request.from_url(f'https://example.com/{i}') for i in range(3)]
    await rq_client.add_batch_of_requests(requests)

    # Re-add the first request (still pending) without `forefront`; it must stay in its original position.
    await rq_client.add_batch_of_requests([requests[0]])

    fetched_urls = []
    while (request := await rq_client.fetch_next_request()) is not None:
        fetched_urls.append(request.url)
        await rq_client.mark_request_as_handled(request)

    assert fetched_urls == [
        'https://example.com/0',
        'https://example.com/1',
        'https://example.com/2',
    ]


async def test_reclaim_modified_request_after_forefront_readd(rq_client: MemoryRequestQueueClient) -> None:
    """Test reclaiming a modified request after it was repositioned to the forefront."""
    request = Request.from_url('https://example.com/page')
    await rq_client.add_batch_of_requests([request])
    await rq_client.add_batch_of_requests([request], forefront=True)

    fetched = await rq_client.fetch_next_request()
    assert fetched is request

    modified = request.model_copy(deep=True)
    modified.user_data['reclaimed'] = True
    await rq_client.reclaim_request(modified)

    assert await rq_client.is_empty() is False
    reclaimed = await rq_client.fetch_next_request()
    assert reclaimed is modified
    assert reclaimed.user_data['reclaimed'] is True
