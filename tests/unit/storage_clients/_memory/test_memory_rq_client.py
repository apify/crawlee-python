from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storage_clients._memory import MemoryRequestQueueClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

pytestmark = pytest.mark.only


@pytest.fixture
async def rq_client() -> AsyncGenerator[MemoryRequestQueueClient, None]:
    """Fixture that provides a fresh memory request queue client for each test."""
    client = await MemoryStorageClient().open_request_queue_client(name='test_rq')
    yield client
    await client.drop()


async def test_open_creates_new_rq() -> None:
    """Test that open() creates a new request queue with proper metadata and adds it to the cache."""
    client = await MemoryStorageClient().open_request_queue_client(name='new_rq')

    # Verify correct client type and properties
    assert isinstance(client, MemoryRequestQueueClient)
    assert client.metadata.id is not None
    assert client.metadata.name == 'new_rq'
    assert isinstance(client.metadata.created_at, datetime)
    assert isinstance(client.metadata.accessed_at, datetime)
    assert isinstance(client.metadata.modified_at, datetime)
    assert client.metadata.handled_request_count == 0
    assert client.metadata.pending_request_count == 0
    assert client.metadata.total_request_count == 0
    assert client.metadata.had_multiple_clients is False

    # Verify the client was cached
    assert 'new_rq' in MemoryRequestQueueClient._cache_by_name


async def test_open_existing_rq(rq_client: MemoryRequestQueueClient) -> None:
    """Test that open() loads an existing request queue with matching properties."""
    configuration = Configuration(purge_on_start=False)
    # Open the same request queue again
    reopened_client = await MemoryStorageClient().open_request_queue_client(
        name=rq_client.metadata.name,
        configuration=configuration,
    )

    # Verify client properties
    assert rq_client.metadata.id == reopened_client.metadata.id
    assert rq_client.metadata.name == reopened_client.metadata.name

    # Verify clients (python) ids
    assert id(rq_client) == id(reopened_client)


async def test_rq_client_purge_on_start() -> None:
    """Test that purge_on_start=True clears existing data in the RQ."""
    configuration = Configuration(purge_on_start=True)

    # Create RQ and add data
    rq_client1 = await MemoryStorageClient().open_request_queue_client(
        name='test_purge_rq',
        configuration=configuration,
    )
    request = Request.from_url(url='https://example.com/initial')
    await rq_client1.add_batch_of_requests([request])

    # Verify request was added
    assert await rq_client1.is_empty() is False

    # Reopen
    rq_client2 = await MemoryStorageClient().open_request_queue_client(
        name='test_purge_rq',
        configuration=configuration,
    )

    # Verify queue was purged
    assert await rq_client2.is_empty() is True


async def test_rq_client_no_purge_on_start() -> None:
    """Test that purge_on_start=False keeps existing data in the RQ."""
    configuration = Configuration(purge_on_start=False)

    # Create RQ and add data
    rq_client1 = await MemoryStorageClient().open_request_queue_client(
        name='test_no_purge_rq',
        configuration=configuration,
    )
    request = Request.from_url(url='https://example.com/preserved')
    await rq_client1.add_batch_of_requests([request])

    # Reopen
    rq_client2 = await MemoryStorageClient().open_request_queue_client(
        name='test_no_purge_rq',
        configuration=configuration,
    )

    # Verify request was preserved
    assert await rq_client2.is_empty() is False
    next_request = await rq_client2.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://example.com/preserved'


async def test_open_with_id_and_name() -> None:
    """Test that open() can be used with both id and name parameters."""
    client = await MemoryStorageClient().open_request_queue_client(
        id='some-id',
        name='some-name',
    )
    assert client.metadata.id is not None  # ID is always auto-generated
    assert client.metadata.name == 'some-name'


async def test_add_batch_of_requests(rq_client: MemoryRequestQueueClient) -> None:
    """Test adding a batch of requests to the queue."""
    requests = [
        Request.from_url(url='https://example.com/1'),
        Request.from_url(url='https://example.com/2'),
        Request.from_url(url='https://example.com/3'),
    ]

    response = await rq_client.add_batch_of_requests(requests)

    # Verify correct response
    assert len(response.processed_requests) == 3
    assert len(response.unprocessed_requests) == 0

    # Verify each request was processed correctly
    for i, req in enumerate(requests):
        assert response.processed_requests[i].id == req.id
        assert response.processed_requests[i].unique_key == req.unique_key
        assert response.processed_requests[i].was_already_present is False
        assert response.processed_requests[i].was_already_handled is False

    # Verify metadata was updated
    assert rq_client.metadata.total_request_count == 3
    assert rq_client.metadata.pending_request_count == 3


async def test_add_batch_of_requests_with_duplicates(rq_client: MemoryRequestQueueClient) -> None:
    """Test adding requests with duplicate unique keys."""
    # Add initial requests
    initial_requests = [
        Request.from_url(url='https://example.com/1', unique_key='key1'),
        Request.from_url(url='https://example.com/2', unique_key='key2'),
    ]
    await rq_client.add_batch_of_requests(initial_requests)

    # Mark first request as handled
    req1 = await rq_client.fetch_next_request()
    assert req1 is not None
    await rq_client.mark_request_as_handled(req1)

    # Add duplicate requests
    duplicate_requests = [
        Request.from_url(url='https://example.com/1-dup', unique_key='key1'),  # Same as first (handled)
        Request.from_url(url='https://example.com/2-dup', unique_key='key2'),  # Same as second (not handled)
        Request.from_url(url='https://example.com/3', unique_key='key3'),      # New request
    ]
    response = await rq_client.add_batch_of_requests(duplicate_requests)

    # Verify response
    assert len(response.processed_requests) == 3

    # First request should be marked as already handled
    assert response.processed_requests[0].was_already_present is True
    assert response.processed_requests[0].was_already_handled is True

    # Second request should be marked as already present but not handled
    assert response.processed_requests[1].was_already_present is True
    assert response.processed_requests[1].was_already_handled is False

    # Third request should be new
    assert response.processed_requests[2].was_already_present is False
    assert response.processed_requests[2].was_already_handled is False


async def test_add_batch_of_requests_to_forefront(rq_client: MemoryRequestQueueClient) -> None:
    """Test adding requests to the forefront of the queue."""
    # Add initial requests
    initial_requests = [
        Request.from_url(url='https://example.com/1'),
        Request.from_url(url='https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(initial_requests)

    # Add new requests to forefront
    forefront_requests = [
        Request.from_url(url='https://example.com/priority'),
    ]
    await rq_client.add_batch_of_requests(forefront_requests, forefront=True)

    # The priority request should be fetched first
    next_request = await rq_client.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://example.com/priority'


async def test_fetch_next_request(rq_client: MemoryRequestQueueClient) -> None:
    """Test fetching the next request from the queue."""
    # Add some requests
    requests = [
        Request.from_url(url='https://example.com/1'),
        Request.from_url(url='https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(requests)

    # Fetch first request
    request1 = await rq_client.fetch_next_request()
    assert request1 is not None
    assert request1.url == 'https://example.com/1'

    # Fetch second request
    request2 = await rq_client.fetch_next_request()
    assert request2 is not None
    assert request2.url == 'https://example.com/2'

    # No more requests
    request3 = await rq_client.fetch_next_request()
    assert request3 is None


async def test_fetch_skips_handled_requests(rq_client: MemoryRequestQueueClient) -> None:
    """Test that fetch_next_request skips handled requests."""
    # Add requests
    requests = [
        Request.from_url(url='https://example.com/1'),
        Request.from_url(url='https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(requests)

    # Fetch and handle first request
    request1 = await rq_client.fetch_next_request()
    assert request1 is not None
    await rq_client.mark_request_as_handled(request1)

    # Next fetch should return second request, not the handled one
    request = await rq_client.fetch_next_request()
    assert request is not None
    assert request.url == 'https://example.com/2'


async def test_fetch_skips_in_progress_requests(rq_client: MemoryRequestQueueClient) -> None:
    """Test that fetch_next_request skips requests that are already in progress."""
    # Add requests
    requests = [
        Request.from_url(url='https://example.com/1'),
        Request.from_url(url='https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(requests)

    # Fetch first request (it should be in progress now)
    request1 = await rq_client.fetch_next_request()
    assert request1 is not None

    # Next fetch should return second request, not the in-progress one
    request2 = await rq_client.fetch_next_request()
    assert request2 is not None
    assert request2.url == 'https://example.com/2'

    # Third fetch should return None as all requests are in progress
    request3 = await rq_client.fetch_next_request()
    assert request3 is None


async def test_get_request(rq_client: MemoryRequestQueueClient) -> None:
    """Test getting a request by ID."""
    # Add a request
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Get the request by ID
    retrieved_request = await rq_client.get_request(request.id)
    assert retrieved_request is not None
    assert retrieved_request.id == request.id
    assert retrieved_request.url == request.url

    # Try to get a non-existent request
    nonexistent = await rq_client.get_request('nonexistent-id')
    assert nonexistent is None


async def test_get_in_progress_request(rq_client: MemoryRequestQueueClient) -> None:
    """Test getting an in-progress request by ID."""
    # Add a request
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Fetch the request to make it in-progress
    fetched = await rq_client.fetch_next_request()
    assert fetched is not None

    # Get the request by ID
    retrieved = await rq_client.get_request(request.id)
    assert retrieved is not None
    assert retrieved.id == request.id
    assert retrieved.url == request.url


async def test_mark_request_as_handled(rq_client: MemoryRequestQueueClient) -> None:
    """Test marking a request as handled."""
    # Add a request
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Fetch the request to make it in-progress
    fetched = await rq_client.fetch_next_request()
    assert fetched is not None

    # Mark as handled
    result = await rq_client.mark_request_as_handled(fetched)
    assert result is not None
    assert result.id == fetched.id
    assert result.was_already_handled is True

    # Check that metadata was updated
    assert rq_client.metadata.handled_request_count == 1
    assert rq_client.metadata.pending_request_count == 0

    # Try to mark again (should fail as it's no longer in-progress)
    result = await rq_client.mark_request_as_handled(fetched)
    assert result is None


async def test_reclaim_request(rq_client: MemoryRequestQueueClient) -> None:
    """Test reclaiming a request back to the queue."""
    # Add a request
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Fetch the request to make it in-progress
    fetched = await rq_client.fetch_next_request()
    assert fetched is not None

    # Reclaim the request
    result = await rq_client.reclaim_request(fetched)
    assert result is not None
    assert result.id == fetched.id
    assert result.was_already_handled is False

    # It should be available to fetch again
    reclaimed = await rq_client.fetch_next_request()
    assert reclaimed is not None
    assert reclaimed.id == fetched.id


async def test_reclaim_request_to_forefront(rq_client: MemoryRequestQueueClient) -> None:
    """Test reclaiming a request to the forefront of the queue."""
    # Add requests
    requests = [
        Request.from_url(url='https://example.com/1'),
        Request.from_url(url='https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(requests)

    # Fetch the second request to make it in-progress
    await rq_client.fetch_next_request()  # Skip the first one
    request2 = await rq_client.fetch_next_request()
    assert request2 is not None
    assert request2.url == 'https://example.com/2'

    # Reclaim the request to forefront
    await rq_client.reclaim_request(request2, forefront=True)

    # It should now be the first in the queue
    next_request = await rq_client.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://example.com/2'


async def test_is_empty(rq_client: MemoryRequestQueueClient) -> None:
    """Test checking if the queue is empty."""
    # Initially empty
    assert await rq_client.is_empty() is True

    # Add a request
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Not empty now
    assert await rq_client.is_empty() is False

    # Fetch and handle
    fetched = await rq_client.fetch_next_request()
    assert fetched is not None
    await rq_client.mark_request_as_handled(fetched)

    # Empty again (all requests handled)
    assert await rq_client.is_empty() is True


async def test_is_empty_with_in_progress(rq_client: MemoryRequestQueueClient) -> None:
    """Test that in-progress requests don't affect is_empty."""
    # Add a request
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Fetch but don't handle
    await rq_client.fetch_next_request()

    # Queue should still be considered non-empty
    # This is because the request hasn't been handled yet
    assert await rq_client.is_empty() is False


async def test_drop(rq_client: MemoryRequestQueueClient) -> None:
    """Test that drop removes the queue from cache and clears all data."""
    # Add a request
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Verify the queue exists in the cache
    assert rq_client.metadata.name in MemoryRequestQueueClient._cache_by_name

    # Drop the queue
    await rq_client.drop()

    # Verify the queue was removed from the cache
    assert rq_client.metadata.name not in MemoryRequestQueueClient._cache_by_name

    # Verify the queue is empty
    assert await rq_client.is_empty() is True


async def test_metadata_updates(rq_client: MemoryRequestQueueClient) -> None:
    """Test that operations properly update metadata timestamps."""
    # Record initial timestamps
    initial_created = rq_client.metadata.created_at
    initial_accessed = rq_client.metadata.accessed_at
    initial_modified = rq_client.metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates modified_at and accessed_at
    request = Request.from_url(url='https://example.com/test')
    await rq_client.add_batch_of_requests([request])

    # Verify timestamps
    assert rq_client.metadata.created_at == initial_created
    assert rq_client.metadata.modified_at > initial_modified
    assert rq_client.metadata.accessed_at > initial_accessed

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Record timestamps after add
    accessed_after_add = rq_client.metadata.accessed_at
    modified_after_add = rq_client.metadata.modified_at

    # Check is_empty (should only update accessed_at)
    await rq_client.is_empty()

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Verify only accessed_at changed
    assert rq_client.metadata.modified_at == modified_after_add
    assert rq_client.metadata.accessed_at > accessed_after_add


async def test_unique_key_generation(rq_client: MemoryRequestQueueClient) -> None:
    """Test that unique keys are auto-generated if not provided."""
    # Add requests without explicit unique keys
    requests = [
        Request.from_url(url='https://example.com/1'),
        Request.from_url(url='https://example.com/1', always_enqueue=True)
    ]
    response = await rq_client.add_batch_of_requests(requests)

    # Both should be added as their auto-generated unique keys will differ
    assert len(response.processed_requests) == 2
    assert all(not pr.was_already_present for pr in response.processed_requests)

    # Add a request with explicit unique key
    request = Request.from_url(url='https://example.com/2', unique_key='explicit-key')
    await rq_client.add_batch_of_requests([request])

    # Add duplicate with same unique key
    duplicate = Request.from_url(url='https://example.com/different', unique_key='explicit-key')
    duplicate_response = await rq_client.add_batch_of_requests([duplicate])

    # Should be marked as already present
    assert duplicate_response.processed_requests[0].was_already_present is True
