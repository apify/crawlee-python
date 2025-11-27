from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from crawlee import Request, service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient, StorageClient
from crawlee.storages import RequestQueue
from crawlee.storages._storage_instance_manager import StorageInstanceManager

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_clients import StorageClient


@pytest.fixture
async def rq(
    storage_client: StorageClient,
) -> AsyncGenerator[RequestQueue, None]:
    """Fixture that provides a request queue instance for each test."""
    rq = await RequestQueue.open(
        storage_client=storage_client,
    )

    yield rq
    await rq.drop()


async def test_open_creates_new_rq(
    storage_client: StorageClient,
) -> None:
    """Test that open() creates a new request queue with proper metadata."""
    rq = await RequestQueue.open(
        name='new-request-queue',
        storage_client=storage_client,
    )

    # Verify request queue properties
    assert rq.id is not None
    assert rq.name == 'new-request-queue'
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
) -> None:
    """Test that open() raises an error when both id and name are provided."""
    with pytest.raises(
        ValueError,
        match=r'Only one of "id", "name", "alias" can be specified, but following arguments '
        r'were specified: "id", "name".',
    ):
        await RequestQueue.open(
            id='some-id',
            name='some-name',
            storage_client=storage_client,
        )


async def test_open_by_id(
    storage_client: StorageClient,
) -> None:
    """Test opening a request queue by its ID."""
    # First create a request queue by name
    rq1 = await RequestQueue.open(
        name='rq-by-id-test',
        storage_client=storage_client,
    )

    # Add a request to identify it
    await rq1.add_request('https://example.com/open-by-id-test')

    # Open the request queue by ID
    rq2 = await RequestQueue.open(
        id=rq1.id,
        storage_client=storage_client,
    )

    # Verify it's the same request queue
    assert rq2.id == rq1.id
    assert rq2.name == 'rq-by-id-test'

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
    unique_key = added_result.unique_key

    # Retrieve the request by ID
    retrieved_request = await rq.get_request(unique_key)
    assert retrieved_request is not None
    assert retrieved_request.unique_key == unique_key
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
    assert reclaimed_request.unique_key == request.unique_key
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


@pytest.mark.parametrize(
    ('wait_for_all'),
    [
        pytest.param(True, id='wait for all'),
        pytest.param(False, id='do not wait for all'),
    ],
)
async def test_add_requests_wait_for_all(
    rq: RequestQueue,
    *,
    wait_for_all: bool,
) -> None:
    """Test adding requests with wait_for_all_requests_to_be_added option."""
    urls = [f'https://example.com/{i}' for i in range(15)]

    # Add requests without waiting
    await rq.add_requests(
        urls,
        batch_size=5,
        wait_for_all_requests_to_be_added=wait_for_all,
        wait_time_between_batches=timedelta(milliseconds=50),
    )

    if not wait_for_all:
        # Immediately after adding, the total count may be less than 15 due to background processing
        assert await rq.get_total_count() <= 15

        # Wait for background tasks to complete
        while await rq.get_total_count() < 15:  # noqa: ASYNC110
            await asyncio.sleep(0.1)

    # Verify all requests were added
    assert await rq.get_total_count() == 15


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
) -> None:
    """Test dropping a request queue removes it from cache and clears its data."""
    rq = await RequestQueue.open(
        name='drop-test',
        storage_client=storage_client,
    )

    # Add a request
    await rq.add_request('https://example.com')

    # Drop the request queue
    await rq.drop()

    # Verify request queue is empty (by creating a new one with the same name)
    new_rq = await RequestQueue.open(
        name='drop-test',
        storage_client=storage_client,
    )

    # Verify the queue is empty
    assert await new_rq.is_empty() is True
    metadata = await new_rq.get_metadata()
    assert metadata.total_request_count == 0
    assert metadata.pending_request_count == 0
    await new_rq.drop()


async def test_reopen_default(
    storage_client: StorageClient,
) -> None:
    """Test reopening the default request queue."""
    # First clean up any storage instance caches
    storage_instance_manager = service_locator.storage_instance_manager
    storage_instance_manager.clear_cache()

    # Open the default request queue
    rq1 = await RequestQueue.open(
        storage_client=storage_client,
    )

    # If a request queue already exists (due to previous test run), purge it to start fresh
    try:
        await rq1.purge()
    except Exception:
        # If purge fails, try dropping and recreating
        await rq1.drop()
        rq1 = await RequestQueue.open(
            storage_client=storage_client,
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
) -> None:
    """Test purging a request queue removes all requests but keeps the queue itself."""
    # First create a request queue
    rq = await RequestQueue.open(
        name='purge-test-queue',
        storage_client=storage_client,
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
    assert rq.name == 'purge-test-queue'  # Same name preserved

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


async def test_open_with_alias(
    storage_client: StorageClient,
) -> None:
    """Test opening request queues with alias parameter for NDU functionality."""
    # Create request queues with different aliases
    rq_1 = await RequestQueue.open(
        alias='test_alias_1',
        storage_client=storage_client,
    )
    rq_2 = await RequestQueue.open(
        alias='test_alias_2',
        storage_client=storage_client,
    )

    # Verify they have different IDs but no names (unnamed)
    assert rq_1.id != rq_2.id
    assert rq_1.name is None
    assert rq_2.name is None

    # Add different requests to each
    await rq_1.add_request('https://example.com/1')
    await rq_1.add_request('https://example.com/2')
    await rq_2.add_request('https://example.com/3')

    # Verify data isolation
    request_1 = await rq_1.fetch_next_request()
    request_2 = await rq_2.fetch_next_request()

    assert request_1 is not None
    assert request_2 is not None
    assert request_1.url == 'https://example.com/1'
    assert request_2.url == 'https://example.com/3'

    # Clean up
    await rq_1.drop()
    await rq_2.drop()


async def test_alias_caching(
    storage_client: StorageClient,
) -> None:
    """Test that request queues with same alias return same instance (cached)."""
    # Open rq with alias
    rq_1 = await RequestQueue.open(
        alias='cache_test',
        storage_client=storage_client,
    )

    # Open again with same alias
    rq_2 = await RequestQueue.open(
        alias='cache_test',
        storage_client=storage_client,
    )

    # Should be same instance
    assert rq_1 is rq_2
    assert rq_1.id == rq_2.id

    # Clean up
    await rq_1.drop()


async def test_alias_with_id_error(
    storage_client: StorageClient,
) -> None:
    """Test that providing both alias and id raises error."""
    with pytest.raises(
        ValueError,
        match=r'Only one of "id", "name", "alias" can be specified, but following arguments '
        r'were specified: "id", "alias".',
    ):
        await RequestQueue.open(
            id='some-id',
            alias='some-alias',
            storage_client=storage_client,
        )


async def test_alias_with_name_error(
    storage_client: StorageClient,
) -> None:
    """Test that providing both alias and name raises error."""
    with pytest.raises(
        ValueError,
        match=r'Only one of "id", "name", "alias" can be specified, but following arguments '
        r'were specified: "name", "alias".',
    ):
        await RequestQueue.open(
            name='some-name',
            alias='some-alias',
            storage_client=storage_client,
        )


async def test_alias_with_special_characters(
    storage_client: StorageClient,
) -> None:
    """Test alias functionality with special characters."""
    special_aliases = [
        'alias-with-dashes',
        'alias_with_underscores',
        'alias.with.dots',
        'alias123with456numbers',
        'CamelCaseAlias',
    ]

    queues = []
    for alias in special_aliases:
        rq = await RequestQueue.open(
            alias=alias,
            storage_client=storage_client,
        )
        queues.append(rq)

        # Add request with the alias as identifier in URL
        await rq.add_request(f'https://example.com/{alias}')

    # Verify all work correctly
    for i, rq in enumerate(queues):
        request = await rq.fetch_next_request()
        assert request is not None
        assert f'/{special_aliases[i]}' in request.url

    # Clean up
    for rq in queues:
        await rq.drop()


async def test_alias_request_operations(
    storage_client: StorageClient,
) -> None:
    """Test that request operations work correctly with alias queues."""
    rq = await RequestQueue.open(
        alias='request_ops_test',
        storage_client=storage_client,
    )

    # Test adding multiple requests
    urls = [
        'https://example.com/page1',
        'https://example.com/page2',
        'https://example.com/page3',
    ]

    for url in urls:
        result = await rq.add_request(url)
        assert result.was_already_present is False

    # Test queue metadata
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 3
    assert metadata.pending_request_count == 3
    assert metadata.handled_request_count == 0

    # Test fetching and handling requests
    processed_urls = []
    while not await rq.is_empty():
        request = await rq.fetch_next_request()
        if request:
            processed_urls.append(request.url)
            await rq.mark_request_as_handled(request)

    # Verify all requests were processed
    assert len(processed_urls) == 3
    assert set(processed_urls) == set(urls)

    # Verify final state
    metadata = await rq.get_metadata()
    assert metadata.pending_request_count == 0
    assert metadata.handled_request_count == 3
    assert await rq.is_empty() is True

    # Clean up
    await rq.drop()


async def test_alias_forefront_operations(
    storage_client: StorageClient,
) -> None:
    """Test forefront operations work correctly with alias queues."""
    rq = await RequestQueue.open(
        alias='forefront_test',
        storage_client=storage_client,
    )

    # Add normal requests
    await rq.add_request('https://example.com/normal1')
    await rq.add_request('https://example.com/normal2')

    # Add priority request to forefront
    await rq.add_request('https://example.com/priority', forefront=True)

    # Priority request should come first
    priority_request = await rq.fetch_next_request()
    assert priority_request is not None
    assert priority_request.url == 'https://example.com/priority'

    # Then normal requests
    normal_request = await rq.fetch_next_request()
    assert normal_request is not None
    assert normal_request.url == 'https://example.com/normal1'

    # Clean up
    await rq.drop()


async def test_alias_batch_operations(
    storage_client: StorageClient,
) -> None:
    """Test batch operations work correctly with alias queues."""
    rq = await RequestQueue.open(
        alias='batch_test',
        storage_client=storage_client,
    )

    # Test batch adding
    batch_urls = [
        'https://example.com/batch1',
        'https://example.com/batch2',
        'https://example.com/batch3',
    ]

    await rq.add_requests(batch_urls)

    # Wait for background processing
    await asyncio.sleep(0.1)

    # Verify all requests were added
    metadata = await rq.get_metadata()
    assert metadata.total_request_count == 3

    # Clean up
    await rq.drop()


async def test_named_vs_alias_conflict_detection(
    storage_client: StorageClient,
) -> None:
    """Test that conflicts between named and alias storages are detected."""
    # Test 1: Create named storage first, then try alias with same name
    named_rq = await RequestQueue.open(
        name='conflict-test',
        storage_client=storage_client,
    )
    assert named_rq.name == 'conflict-test'

    # Try to create alias with same name - should raise error
    with pytest.raises(ValueError, match=r'Cannot create alias storage "conflict-test".*already exists'):
        await RequestQueue.open(alias='conflict-test', storage_client=storage_client)

    # Clean up
    await named_rq.drop()

    # Test 2: Create alias first, then try named with same name
    alias_rq = await RequestQueue.open(alias='conflict-test2', storage_client=storage_client)
    assert alias_rq.name is None  # Alias storages have no name

    # Try to create named with same name - should raise error
    with pytest.raises(ValueError, match=r'Cannot create named storage "conflict-test2".*already exists'):
        await RequestQueue.open(name='conflict-test2', storage_client=storage_client)

    # Clean up
    await alias_rq.drop()

    # Test 3: Different names should work fine
    named_rq_ok = await RequestQueue.open(name='different-name')
    alias_rq_ok = await RequestQueue.open(alias='different-alias')

    assert named_rq_ok.name == 'different-name'
    assert alias_rq_ok.name is None

    # Clean up
    await named_rq_ok.drop()
    await alias_rq_ok.drop()


async def test_alias_parameter(
    storage_client: StorageClient,
) -> None:
    """Test request queue creation and operations with alias parameter."""
    # Create request queue with alias
    alias_rq = await RequestQueue.open(
        alias='test_alias',
        storage_client=storage_client,
    )

    # Verify alias request queue properties
    assert alias_rq.id is not None
    assert alias_rq.name is None  # Alias storages should be unnamed

    # Test data operations
    await alias_rq.add_request('https://example.com/alias')
    metadata = await alias_rq.get_metadata()
    assert metadata.pending_request_count == 1

    await alias_rq.drop()


async def test_alias_vs_named_isolation(
    storage_client: StorageClient,
) -> None:
    """Test that alias and named request queues with same identifier are isolated."""
    # Create named request queue
    named_rq = await RequestQueue.open(
        name='test-identifier',
        storage_client=storage_client,
    )

    # Verify named request queue
    assert named_rq.name == 'test-identifier'
    await named_rq.add_request('https://named.example.com')

    # Clean up named request queue first
    await named_rq.drop()

    # Now create alias request queue with same identifier (should work after cleanup)
    alias_rq = await RequestQueue.open(
        alias='test-identifier',
        storage_client=storage_client,
    )

    # Should be different instance
    assert alias_rq.name is None
    await alias_rq.add_request('https://alias.example.com')

    # Verify alias data
    alias_request = await alias_rq.fetch_next_request()
    assert alias_request is not None
    assert alias_request.url == 'https://alias.example.com'

    await alias_rq.drop()


async def test_default_vs_alias_default_equivalence(
    storage_client: StorageClient,
) -> None:
    """Test that default request queue and alias='default' are equivalent."""
    # Open default request queue
    default_rq = await RequestQueue.open(
        storage_client=storage_client,
    )

    alias_default_rq = await RequestQueue.open(
        alias=StorageInstanceManager._DEFAULT_STORAGE_ALIAS,
        storage_client=storage_client,
    )

    # Should be the same
    assert default_rq.id == alias_default_rq.id
    assert default_rq.name is None
    assert alias_default_rq.name is None

    # Data should be shared
    await default_rq.add_request('https://default.example.com')
    metadata = await alias_default_rq.get_metadata()
    assert metadata.pending_request_count == 1

    await default_rq.drop()


async def test_multiple_alias_isolation(
    storage_client: StorageClient,
) -> None:
    """Test that different aliases create separate request queues."""
    request_queues = []

    for i in range(3):
        rq = await RequestQueue.open(
            alias=f'alias_{i}',
            storage_client=storage_client,
        )
        await rq.add_request(f'https://example.com/alias_{i}')
        request_queues.append(rq)

    # All should be different
    for i in range(3):
        for j in range(i + 1, 3):
            assert request_queues[i].id != request_queues[j].id

    # Verify data isolation
    for i, rq in enumerate(request_queues):
        request = await rq.fetch_next_request()
        assert request is not None
        assert request.url == f'https://example.com/alias_{i}'
        await rq.drop()


async def test_purge_on_start_enabled(storage_client: StorageClient) -> None:
    """Test purge behavior when purge_on_start=True: named storages retain data, unnamed storages are purged."""

    # Skip this test for memory storage since it doesn't persist data between client instances.
    if isinstance(storage_client, MemoryStorageClient):
        pytest.skip('Memory storage does not persist data between client instances.')

    configuration = Configuration(purge_on_start=True)

    # First, create all storage types with purge enabled and add data.
    default_rq = await RequestQueue.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    alias_rq = await RequestQueue.open(
        alias='purge-test-alias',
        storage_client=storage_client,
        configuration=configuration,
    )

    named_rq = await RequestQueue.open(
        name='purge-test-named',
        storage_client=storage_client,
        configuration=configuration,
    )

    await default_rq.add_requests(
        [
            'https://default.example.com/1',
            'https://default.example.com/2',
            'https://default.example.com/3',
        ]
    )
    await alias_rq.add_requests(
        [
            'https://alias.example.com/1',
            'https://alias.example.com/2',
            'https://alias.example.com/3',
        ]
    )
    await named_rq.add_requests(
        [
            'https://named.example.com/1',
            'https://named.example.com/2',
            'https://named.example.com/3',
        ]
    )

    default_request = await default_rq.fetch_next_request()
    alias_request = await alias_rq.fetch_next_request()
    named_request = await named_rq.fetch_next_request()

    assert default_request is not None
    assert alias_request is not None
    assert named_request is not None

    await default_rq.mark_request_as_handled(default_request)
    await alias_rq.mark_request_as_handled(alias_request)
    await named_rq.mark_request_as_handled(named_request)

    # Verify data was added
    default_metadata = await default_rq.get_metadata()
    alias_metadata = await alias_rq.get_metadata()
    named_metadata = await named_rq.get_metadata()

    assert default_metadata.pending_request_count == 2
    assert alias_metadata.pending_request_count == 2
    assert named_metadata.pending_request_count == 2

    assert default_metadata.handled_request_count == 1
    assert alias_metadata.handled_request_count == 1
    assert named_metadata.handled_request_count == 1

    assert default_metadata.total_request_count == 3
    assert alias_metadata.total_request_count == 3
    assert named_metadata.total_request_count == 3

    # Verify that default and alias storages are unnamed
    assert default_metadata.name is None
    assert alias_metadata.name is None
    assert named_metadata.name == 'purge-test-named'

    # Clear storage cache to simulate "reopening" storages
    service_locator.storage_instance_manager.clear_cache()

    # Now "reopen" all storages
    default_rq_2 = await RequestQueue.open(
        storage_client=storage_client,
        configuration=configuration,
    )
    alias_rq_2 = await RequestQueue.open(
        alias='purge-test-alias',
        storage_client=storage_client,
        configuration=configuration,
    )
    named_rq_2 = await RequestQueue.open(
        name='purge-test-named',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Check the data after purge
    default_metadata_after = await default_rq_2.get_metadata()
    alias_metadata_after = await alias_rq_2.get_metadata()
    named_metadata_after = await named_rq_2.get_metadata()

    # Unnamed storages (alias and default) should be purged (data removed)
    assert default_metadata_after.pending_request_count == 0
    assert alias_metadata_after.pending_request_count == 0
    assert named_metadata_after.pending_request_count == 2

    assert default_metadata_after.handled_request_count == 1
    assert alias_metadata_after.handled_request_count == 1
    assert named_metadata_after.handled_request_count == 1

    assert default_metadata_after.total_request_count == 3
    assert alias_metadata_after.total_request_count == 3
    assert named_metadata_after.total_request_count == 3

    # Clean up
    await named_rq_2.drop()
    await alias_rq_2.drop()
    await default_rq_2.drop()


async def test_purge_on_start_disabled(storage_client: StorageClient) -> None:
    """Test purge behavior when purge_on_start=False: all storages retain data regardless of type."""

    # Skip this test for memory storage since it doesn't persist data between client instances.
    if isinstance(storage_client, MemoryStorageClient):
        pytest.skip('Memory storage does not persist data between client instances.')

    configuration = Configuration(purge_on_start=False)

    # First, create all storage types with purge disabled and add data.
    default_rq = await RequestQueue.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    alias_rq = await RequestQueue.open(
        alias='purge-test-alias',
        storage_client=storage_client,
        configuration=configuration,
    )

    named_rq = await RequestQueue.open(
        name='purge-test-named',
        storage_client=storage_client,
        configuration=configuration,
    )

    await default_rq.add_requests(
        [
            'https://default.example.com/1',
            'https://default.example.com/2',
            'https://default.example.com/3',
        ]
    )
    await alias_rq.add_requests(
        [
            'https://alias.example.com/1',
            'https://alias.example.com/2',
            'https://alias.example.com/3',
        ]
    )
    await named_rq.add_requests(
        [
            'https://named.example.com/1',
            'https://named.example.com/2',
            'https://named.example.com/3',
        ]
    )

    default_request = await default_rq.fetch_next_request()
    alias_request = await alias_rq.fetch_next_request()
    named_request = await named_rq.fetch_next_request()

    assert default_request is not None
    assert alias_request is not None
    assert named_request is not None

    await default_rq.mark_request_as_handled(default_request)
    await alias_rq.mark_request_as_handled(alias_request)
    await named_rq.mark_request_as_handled(named_request)

    # Verify data was added
    default_metadata = await default_rq.get_metadata()
    alias_metadata = await alias_rq.get_metadata()
    named_metadata = await named_rq.get_metadata()

    assert default_metadata.pending_request_count == 2
    assert alias_metadata.pending_request_count == 2
    assert named_metadata.pending_request_count == 2

    assert default_metadata.handled_request_count == 1
    assert alias_metadata.handled_request_count == 1
    assert named_metadata.handled_request_count == 1

    assert default_metadata.total_request_count == 3
    assert alias_metadata.total_request_count == 3
    assert named_metadata.total_request_count == 3

    # Verify that default and alias storages are unnamed
    assert default_metadata.name is None
    assert alias_metadata.name is None
    assert named_metadata.name == 'purge-test-named'

    # Clear storage cache to simulate "reopening" storages
    service_locator.storage_instance_manager.clear_cache()

    # Now "reopen" all storages
    default_rq_2 = await RequestQueue.open(
        storage_client=storage_client,
        configuration=configuration,
    )
    alias_rq_2 = await RequestQueue.open(
        alias='purge-test-alias',
        storage_client=storage_client,
        configuration=configuration,
    )
    named_rq_2 = await RequestQueue.open(
        name='purge-test-named',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Check the data after purge
    default_metadata_after = await default_rq_2.get_metadata()
    alias_metadata_after = await alias_rq_2.get_metadata()
    named_metadata_after = await named_rq_2.get_metadata()

    # Unnamed storages (alias and default) should be purged (data removed)
    assert default_metadata_after.pending_request_count == 2
    assert alias_metadata_after.pending_request_count == 2
    assert named_metadata_after.pending_request_count == 2

    assert default_metadata_after.handled_request_count == 1
    assert alias_metadata_after.handled_request_count == 1
    assert named_metadata_after.handled_request_count == 1

    assert default_metadata_after.total_request_count == 3
    assert alias_metadata_after.total_request_count == 3
    assert named_metadata_after.total_request_count == 3

    # Clean up
    await named_rq_2.drop()
    await alias_rq_2.drop()
    await default_rq_2.drop()


async def test_name_default_not_allowed(storage_client: StorageClient) -> None:
    """Test that storage can't have default alias as name, to prevent collisions with unnamed storage alias."""
    with pytest.raises(
        ValueError,
        match=f'Storage name cannot be "{StorageInstanceManager._DEFAULT_STORAGE_ALIAS}" as '
        f'it is reserved for default alias.',
    ):
        await RequestQueue.open(name=StorageInstanceManager._DEFAULT_STORAGE_ALIAS, storage_client=storage_client)


@pytest.mark.parametrize(
    ('name', 'is_valid'),
    [
        pytest.param('F', True, id='single-char'),
        pytest.param('7', True, id='single-digit'),
        pytest.param('FtghdfseySds', True, id='mixed-case'),
        pytest.param('125673450', True, id='all-digits'),
        pytest.param('Ft2134Sfe0O1hf', True, id='mixed-alphanumeric'),
        pytest.param('name-with-dashes', True, id='dashes'),
        pytest.param('1-value', True, id='number start'),
        pytest.param('value-1', True, id='number end'),
        pytest.param('test-1-value', True, id='number middle'),
        pytest.param('test-------value', True, id='multiple-dashes'),
        pytest.param('test-VALUES-test', True, id='multiple-cases'),
        pytest.param('name_with_underscores', False, id='underscores'),
        pytest.param('name with spaces', False, id='spaces'),
        pytest.param('-test', False, id='dashes start'),
        pytest.param('test-', False, id='dashes end'),
    ],
)
async def test_validate_name(storage_client: StorageClient, name: str, *, is_valid: bool) -> None:
    """Test name validation logic."""
    if is_valid:
        # Should not raise
        dataset = await RequestQueue.open(name=name, storage_client=storage_client)
        assert dataset.name == name
        await dataset.drop()
    else:
        with pytest.raises(ValueError, match=rf'Invalid storage name "{name}".*'):
            await RequestQueue.open(name=name, storage_client=storage_client)


async def test_reclaim_request_with_change_state(rq: RequestQueue) -> None:
    """Test reclaiming a request and changing its state."""
    # Add a request
    await rq.add_request(Request.from_url('https://example.com/original', user_data={'state': 'original'}))

    # Fetch the request
    request = await rq.fetch_next_request()
    assert request is not None
    assert request.url == 'https://example.com/original'
    assert request.user_data['state'] == 'original'

    # Reclaim the request with modified user data
    request.user_data['state'] = 'modified'
    result = await rq.reclaim_request(request)
    assert result is not None
    assert result.was_already_handled is False

    # Fetch the reclaimed request
    reclaimed_request = await rq.fetch_next_request()
    assert reclaimed_request is not None
    assert reclaimed_request.url == 'https://example.com/original'
    assert reclaimed_request.user_data['state'] == 'modified'


async def test_request_with_noascii_chars(rq: RequestQueue) -> None:
    """Test handling requests with non-ASCII characters in user data."""
    data_with_special_chars = {
        'record_1': 'Supermaxi El Jardín',
        'record_2': 'záznam dva',
        'record_3': '記録三',
    }
    init_request = Request.from_url('https://crawlee.dev', user_data=data_with_special_chars)

    # Add a request with special user data
    await rq.add_request(init_request)

    # Get the request and verify
    request = await rq.fetch_next_request()
    assert request is not None
    assert request.url == 'https://crawlee.dev'
    assert request.user_data == init_request.user_data
