# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee import Request, service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient, StorageClient
from crawlee.storages import RequestQueue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_clients import StorageClient


@pytest.fixture(params=['memory', 'file_system'])
def storage_client(request: pytest.FixtureRequest) -> StorageClient:
    """Parameterized fixture to test with different storage clients."""
    if request.param == 'memory':
        return MemoryStorageClient()

    return FileSystemStorageClient()


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Provide a configuration with a temporary storage directory."""
    return Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )


@pytest.fixture
async def rq(
    storage_client: StorageClient,
    configuration: Configuration,
) -> AsyncGenerator[RequestQueue, None]:
    """Fixture that provides a request queue instance for each test."""
    rq = await RequestQueue.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    yield rq
    await rq.drop()


async def test_open_creates_new_rq(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test that open() creates a new request queue with proper metadata."""
    rq = await RequestQueue.open(
        name='new_request_queue',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify request queue properties
    assert rq.id is not None
    assert rq.name == 'new_request_queue'
    metadata = await rq.get_metadata()
    assert metadata.pending_request_count == 0
    assert metadata.handled_request_count == 0
    assert metadata.total_request_count == 0

    await rq.drop()


async def test_open_existing_rq(
    rq: RequestQueue,
    storage_client: StorageClient,
) -> None:
    """Test that open() loads an existing request queue correctly."""
    # Open the same request queue again
    reopened_rq = await RequestQueue.open(
        name=rq.name,
        storage_client=storage_client,
    )

    # Verify request queue properties
    assert rq.id == reopened_rq.id
    assert rq.name == reopened_rq.name

    # Verify they are the same object (from cache)
    assert id(rq) == id(reopened_rq)


async def test_open_with_id_and_name(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test that open() raises an error when both id and name are provided."""
    with pytest.raises(ValueError, match='Only one of "id" or "name" can be specified'):
        await RequestQueue.open(
            id='some-id',
            name='some-name',
            storage_client=storage_client,
            configuration=configuration,
        )


async def test_open_by_id(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test opening a request queue by its ID."""
    # First create a request queue by name
    rq1 = await RequestQueue.open(
        name='rq_by_id_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Add a request to identify it
    await rq1.add_request('https://example.com/open-by-id-test')

    # Open the request queue by ID
    rq2 = await RequestQueue.open(
        id=rq1.id,
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify it's the same request queue
    assert rq2.id == rq1.id
    assert rq2.name == 'rq_by_id_test'

    # Verify the request is still there
    request = await rq2.fetch_next_request()
    assert request is not None
    assert request.url == 'https://example.com/open-by-id-test'

    # Clean up
    await rq2.drop()


async def test_add_request_string_url(rq: RequestQueue) -> None:
    """Test adding a request with a string URL."""
    # Add a request with a string URL
    url = 'https://example.com'
    result = await rq.add_request(url)

    # Verify request was added
    assert result.id is not None
    assert result.unique_key is not None
    assert result.was_already_present is False
    assert result.was_already_handled is False

    # Verify the queue stats were updated
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 1
    assert metadata.pending_request_count == 1


async def test_add_request_object(rq: RequestQueue) -> None:
    """Test adding a request object."""
    # Create and add a request object
    request = Request.from_url(url='https://example.com', user_data={'key': 'value'})
    result = await rq.add_request(request)

    # Verify request was added
    assert result.id is not None
    assert result.unique_key is not None
    assert result.was_already_present is False
    assert result.was_already_handled is False

    # Verify the queue stats were updated
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 1
    assert metadata.pending_request_count == 1


async def test_add_duplicate_request(rq: RequestQueue) -> None:
    """Test adding a duplicate request to the queue."""
    # Add a request
    url = 'https://example.com'
    first_result = await rq.add_request(url)

    # Add the same request again
    second_result = await rq.add_request(url)

    # Verify the second request was detected as duplicate
    assert second_result.was_already_present is True
    assert second_result.unique_key == first_result.unique_key

    # Verify the queue stats weren't incremented twice
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 1
    assert metadata.pending_request_count == 1


async def test_add_requests_batch(rq: RequestQueue) -> None:
    """Test adding multiple requests in a batch."""
    # Create a batch of requests
    urls = [
        'https://example.com/page1',
        'https://example.com/page2',
        'https://example.com/page3',
    ]

    # Add the requests
    await rq.add_requests(urls)

    # Wait for all background tasks to complete
    await asyncio.sleep(0.1)

    # Verify the queue stats
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 3
    assert metadata.pending_request_count == 3


async def test_add_requests_batch_with_forefront(rq: RequestQueue) -> None:
    """Test adding multiple requests in a batch with forefront option."""
    # Add some initial requests
    await rq.add_request('https://example.com/page1')
    await rq.add_request('https://example.com/page2')

    # Add a batch of priority requests at the forefront

    await rq.add_requests(
        [
            'https://example.com/priority1',
            'https://example.com/priority2',
            'https://example.com/priority3',
        ],
        forefront=True,
    )

    # Wait for all background tasks to complete
    await asyncio.sleep(0.1)

    # Fetch requests - they should come out in priority order first
    next_request1 = await rq.fetch_next_request()
    assert next_request1 is not None
    assert next_request1.url.startswith('https://example.com/priority')

    next_request2 = await rq.fetch_next_request()
    assert next_request2 is not None
    assert next_request2.url.startswith('https://example.com/priority')

    next_request3 = await rq.fetch_next_request()
    assert next_request3 is not None
    assert next_request3.url.startswith('https://example.com/priority')

    # Now we should get the original requests
    next_request4 = await rq.fetch_next_request()
    assert next_request4 is not None
    assert next_request4.url == 'https://example.com/page1'

    next_request5 = await rq.fetch_next_request()
    assert next_request5 is not None
    assert next_request5.url == 'https://example.com/page2'

    # Queue should be empty now
    next_request6 = await rq.fetch_next_request()
    assert next_request6 is None


async def test_add_requests_with_forefront(rq: RequestQueue) -> None:
    """Test adding requests to the front of the queue."""
    # Add some initial requests
    await rq.add_request('https://example.com/page1')
    await rq.add_request('https://example.com/page2')

    # Add a priority request at the forefront
    await rq.add_request('https://example.com/priority', forefront=True)

    # Fetch the next request - should be the priority one
    next_request = await rq.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://example.com/priority'


async def test_add_requests_mixed_forefront(rq: RequestQueue) -> None:
    """Test the ordering when adding requests with mixed forefront values."""
    # Add normal requests
    await rq.add_request('https://example.com/normal1')
    await rq.add_request('https://example.com/normal2')

    # Add a batch with forefront=True
    await rq.add_requests(
        ['https://example.com/priority1', 'https://example.com/priority2'],
        forefront=True,
    )

    # Add another normal request
    await rq.add_request('https://example.com/normal3')

    # Add another priority request
    await rq.add_request('https://example.com/priority3', forefront=True)

    # Wait for background tasks
    await asyncio.sleep(0.1)

    # The expected order should be:
    # 1. priority3 (most recent forefront)
    # 2. priority1 (from batch, forefront)
    # 3. priority2 (from batch, forefront)
    # 4. normal1 (oldest normal)
    # 5. normal2
    # 6. normal3 (newest normal)

    requests = []
    while True:
        req = await rq.fetch_next_request()
        if req is None:
            break
        requests.append(req)
        await rq.mark_request_as_handled(req)

    assert len(requests) == 6
    assert requests[0].url == 'https://example.com/priority3'

    # The next two should be from the forefront batch (exact order within batch may vary)
    batch_urls = {requests[1].url, requests[2].url}
    assert 'https://example.com/priority1' in batch_urls
    assert 'https://example.com/priority2' in batch_urls

    # Then the normal requests in order
    assert requests[3].url == 'https://example.com/normal1'
    assert requests[4].url == 'https://example.com/normal2'
    assert requests[5].url == 'https://example.com/normal3'


async def test_fetch_next_request_and_mark_handled(rq: RequestQueue) -> None:
    """Test fetching and marking requests as handled."""
    # Add some requests
    await rq.add_request('https://example.com/page1')
    await rq.add_request('https://example.com/page2')

    # Fetch first request
    request1 = await rq.fetch_next_request()
    assert request1 is not None
    assert request1.url == 'https://example.com/page1'

    # Mark the request as handled
    result = await rq.mark_request_as_handled(request1)
    assert result is not None
    assert result.was_already_handled is True

    # Fetch next request
    request2 = await rq.fetch_next_request()
    assert request2 is not None
    assert request2.url == 'https://example.com/page2'

    # Mark the second request as handled
    await rq.mark_request_as_handled(request2)

    # Verify counts
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 2
    assert metadata.handled_request_count == 2
    assert metadata.pending_request_count == 0

    # Verify queue is empty
    empty_request = await rq.fetch_next_request()
    assert empty_request is None


async def test_get_request_by_id(rq: RequestQueue) -> None:
    """Test retrieving a request by its ID."""
    # Add a request
    added_result = await rq.add_request('https://example.com')
    request_id = added_result.id

    # Retrieve the request by ID
    retrieved_request = await rq.get_request(request_id)
    assert retrieved_request is not None
    assert retrieved_request.id == request_id
    assert retrieved_request.url == 'https://example.com'


async def test_get_non_existent_request(rq: RequestQueue) -> None:
    """Test retrieving a request that doesn't exist."""
    non_existent_request = await rq.get_request('non-existent-id')
    assert non_existent_request is None


async def test_reclaim_request(rq: RequestQueue) -> None:
    """Test reclaiming a request that failed processing."""
    # Add a request
    await rq.add_request('https://example.com')

    # Fetch the request
    request = await rq.fetch_next_request()
    assert request is not None

    # Reclaim the request
    result = await rq.reclaim_request(request)
    assert result is not None
    assert result.was_already_handled is False

    # Verify we can fetch it again
    reclaimed_request = await rq.fetch_next_request()
    assert reclaimed_request is not None
    assert reclaimed_request.id == request.id
    assert reclaimed_request.url == 'https://example.com'


async def test_reclaim_request_with_forefront(rq: RequestQueue) -> None:
    """Test reclaiming a request to the front of the queue."""
    # Add requests
    await rq.add_request('https://example.com/first')
    await rq.add_request('https://example.com/second')

    # Fetch the first request
    first_request = await rq.fetch_next_request()
    assert first_request is not None
    assert first_request.url == 'https://example.com/first'

    # Reclaim it to the forefront
    await rq.reclaim_request(first_request, forefront=True)

    # The reclaimed request should be returned first (before the second request)
    next_request = await rq.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://example.com/first'


async def test_is_empty(rq: RequestQueue) -> None:
    """Test checking if a request queue is empty."""
    # Initially the queue should be empty
    assert await rq.is_empty() is True

    # Add a request
    await rq.add_request('https://example.com')
    assert await rq.is_empty() is False

    # Fetch and handle the request
    request = await rq.fetch_next_request()

    assert request is not None
    await rq.mark_request_as_handled(request)

    # Queue should be empty again
    assert await rq.is_empty() is True


async def test_is_finished(rq: RequestQueue) -> None:
    """Test checking if a request queue is finished."""
    # Initially the queue should be finished (empty and no background tasks)
    assert await rq.is_finished() is True

    # Add a request
    await rq.add_request('https://example.com')
    assert await rq.is_finished() is False

    # Add requests in the background
    await rq.add_requests(
        ['https://example.com/1', 'https://example.com/2'],
        wait_for_all_requests_to_be_added=False,
    )

    # Queue shouldn't be finished while background tasks are running
    assert await rq.is_finished() is False

    # Wait for background tasks to finish
    await asyncio.sleep(0.2)

    # Process all requests
    while True:
        request = await rq.fetch_next_request()
        if request is None:
            break
        await rq.mark_request_as_handled(request)

    # Now queue should be finished
    assert await rq.is_finished() is True


async def test_mark_non_existent_request_as_handled(rq: RequestQueue) -> None:
    """Test marking a non-existent request as handled."""
    # Create a request that hasn't been added to the queue
    request = Request.from_url(url='https://example.com', id='non-existent-id')

    # Attempt to mark it as handled
    result = await rq.mark_request_as_handled(request)
    assert result is None


async def test_reclaim_non_existent_request(rq: RequestQueue) -> None:
    """Test reclaiming a non-existent request."""
    # Create a request that hasn't been added to the queue
    request = Request.from_url(url='https://example.com', id='non-existent-id')

    # Attempt to reclaim it
    result = await rq.reclaim_request(request)
    assert result is None


async def test_drop(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test dropping a request queue removes it from cache and clears its data."""
    rq = await RequestQueue.open(
        name='drop_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Add a request
    await rq.add_request('https://example.com')

    # Drop the request queue
    await rq.drop()

    # Verify request queue is empty (by creating a new one with the same name)
    new_rq = await RequestQueue.open(
        name='drop_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify the queue is empty
    assert await new_rq.is_empty() is True
    metadata = await new_rq.get_metadata()
    assert metadata.total_request_count == 0
    assert metadata.pending_request_count == 0
    await new_rq.drop()


async def test_reopen_default(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test reopening the default request queue."""
    # First clean up any storage instance caches
    storage_instance_manager = service_locator.storage_instance_manager
    storage_instance_manager.clear_cache()

    # Open the default request queue
    rq1 = await RequestQueue.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    # If a request queue already exists (due to previous test run), purge it to start fresh
    try:
        await rq1.purge()
    except Exception:
        # If purge fails, try dropping and recreating
        await rq1.drop()
        rq1 = await RequestQueue.open(
            storage_client=storage_client,
            configuration=configuration,
        )

    # Verify we're starting fresh
    metadata1 = await rq1.get_metadata()
    assert metadata1.pending_request_count == 0

    # Add a request
    await rq1.add_request('https://example.com/')

    # Verify the request was added
    metadata1 = await rq1.get_metadata()
    assert metadata1.pending_request_count == 1

    # Open the default request queue again
    rq2 = await RequestQueue.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify they are the same queue
    assert rq1.id == rq2.id
    assert rq1.name == rq2.name
    metadata1 = await rq1.get_metadata()
    metadata2 = await rq2.get_metadata()
    assert metadata1.total_request_count == metadata2.total_request_count
    assert metadata1.pending_request_count == metadata2.pending_request_count
    assert metadata1.handled_request_count == metadata2.handled_request_count

    # Verify the request is accessible
    request = await rq2.fetch_next_request()
    assert request is not None
    assert request.url == 'https://example.com/'

    # Clean up after the test
    await rq1.drop()


async def test_purge(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test purging a request queue removes all requests but keeps the queue itself."""
    # First create a request queue
    rq = await RequestQueue.open(
        name='purge_test_queue',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Add some requests
    await rq.add_requests(
        [
            'https://example.com/page1',
            'https://example.com/page2',
            'https://example.com/page3',
        ]
    )

    # Verify requests were added
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 3
    assert metadata.pending_request_count == 3
    assert metadata.handled_request_count == 0

    # Record the queue ID
    queue_id = rq.id

    # Purge the queue
    await rq.purge()

    # Verify the queue still exists but is empty
    assert rq.id == queue_id  # Same ID preserved
    assert rq.name == 'purge_test_queue'  # Same name preserved

    # Queue should be empty now
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 3
    assert metadata.pending_request_count == 0
    assert metadata.handled_request_count == 0
    assert await rq.is_empty() is True

    # Verify we can add new requests after purging
    await rq.add_request('https://example.com/new-after-purge')

    request = await rq.fetch_next_request()
    assert request is not None
    assert request.url == 'https://example.com/new-after-purge'

    # Clean up
    await rq.drop()
