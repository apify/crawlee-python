from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient
from crawlee.storage_clients._file_system import FileSystemRequestQueueClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    return Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )


@pytest.fixture
async def rq_client(configuration: Configuration) -> AsyncGenerator[FileSystemRequestQueueClient, None]:
    """A fixture for a file system request queue client."""
    client = await FileSystemStorageClient().create_rq_client(
        name='test_request_queue',
        configuration=configuration,
    )
    yield client
    await client.drop()


async def test_open_request_queue_by_id(configuration: Configuration) -> None:
    """Test opening a request queue by ID after creating it by name."""
    storage_client = FileSystemStorageClient()

    # First create a request queue by name
    original_client = await storage_client.create_rq_client(
        name='open-by-id-test',
        configuration=configuration,
    )

    # Get the ID from the created client
    rq_id = original_client.metadata.id

    # Add a request to verify it persists
    await original_client.add_batch_of_requests([Request.from_url('https://example.com/test')])

    # Now try to open the same request queue using just the ID
    reopened_client = await storage_client.create_rq_client(
        id=rq_id,
        configuration=configuration,
    )

    # Verify it's the same request queue
    assert reopened_client.metadata.id == rq_id
    assert reopened_client.metadata.name == 'open-by-id-test'

    # Verify the request is still there
    request = await reopened_client.fetch_next_request()
    assert request is not None
    assert request.url == 'https://example.com/test'

    # Clean up
    await reopened_client.drop()


async def test_open_creates_new_rq(configuration: Configuration) -> None:
    """Test that open() creates a new request queue with proper metadata and files on disk."""
    client = await FileSystemStorageClient().create_rq_client(
        name='new_request_queue',
        configuration=configuration,
    )

    # Verify correct client type and properties
    assert isinstance(client, FileSystemRequestQueueClient)
    assert client.metadata.id is not None
    assert client.metadata.name == 'new_request_queue'
    assert client.metadata.handled_request_count == 0
    assert client.metadata.pending_request_count == 0
    assert client.metadata.total_request_count == 0
    assert isinstance(client.metadata.created_at, datetime)
    assert isinstance(client.metadata.accessed_at, datetime)
    assert isinstance(client.metadata.modified_at, datetime)

    # Verify files were created
    assert client.path_to_rq.exists()
    assert client.path_to_metadata.exists()

    # Verify metadata content
    with client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == client.metadata.id
        assert metadata['name'] == 'new_request_queue'


async def test_rq_client_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=True clears existing data in the request queue."""
    configuration.purge_on_start = True

    # Create request queue and add data
    rq_client1 = await FileSystemStorageClient().create_rq_client(configuration=configuration)
    await rq_client1.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify request was added
    assert rq_client1.metadata.pending_request_count == 1
    assert rq_client1.metadata.total_request_count == 1
    assert rq_client1.metadata.handled_request_count == 0

    # Reopen
    rq_client2 = await FileSystemStorageClient().create_rq_client(configuration=configuration)

    # Verify data was purged
    assert rq_client2.metadata.pending_request_count == 0
    assert rq_client2.metadata.total_request_count == 1
    assert rq_client2.metadata.handled_request_count == 0


async def test_rq_client_no_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=False keeps existing data in the request queue."""
    configuration.purge_on_start = False

    # Create request queue and add data
    rq_client1 = await FileSystemStorageClient().create_rq_client(
        name='test-no-purge-rq',
        configuration=configuration,
    )
    await rq_client1.add_batch_of_requests([Request.from_url('https://example.com')])

    # Reopen
    rq_client2 = await FileSystemStorageClient().create_rq_client(
        name='test-no-purge-rq',
        configuration=configuration,
    )

    # Verify data was preserved
    assert rq_client2.metadata.total_request_count == 1


@pytest.fixture
def rq_path(rq_client: FileSystemRequestQueueClient) -> Path:
    """Return the path to the request queue directory."""
    return rq_client.path_to_rq


async def test_add_requests(rq_client: FileSystemRequestQueueClient) -> None:
    """Test adding requests creates proper files in the filesystem."""
    # Add a batch of requests
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    response = await rq_client.add_batch_of_requests(requests)

    # Verify response
    assert len(response.processed_requests) == 3
    for i, processed_request in enumerate(response.processed_requests):
        assert processed_request.unique_key == f'https://example.com/{i + 1}'
        assert processed_request.was_already_present is False
        assert processed_request.was_already_handled is False

    # Verify request files were created
    request_files = list(rq_client.path_to_rq.glob('*.json'))
    assert len(request_files) == 4  # 3 requests + metadata file
    assert rq_client.path_to_metadata in request_files

    # Verify metadata was updated
    assert rq_client.metadata.total_request_count == 3
    assert rq_client.metadata.pending_request_count == 3

    # Verify content of the request files
    for req_file in [f for f in request_files if f != rq_client.path_to_metadata]:
        with req_file.open() as f:
            content = json.load(f)
            assert 'url' in content
            assert content['url'].startswith('https://example.com/')
            assert 'id' in content
            assert content['handled_at'] is None


async def test_add_duplicate_request(rq_client: FileSystemRequestQueueClient) -> None:
    """Test adding a duplicate request."""
    request = Request.from_url('https://example.com')

    # Add the request the first time
    await rq_client.add_batch_of_requests([request])

    # Add the same request again
    second_response = await rq_client.add_batch_of_requests([request])

    # Verify response indicates it was already present
    assert second_response.processed_requests[0].was_already_present is True

    # Verify only one request file exists
    request_files = [f for f in rq_client.path_to_rq.glob('*.json') if f.name != METADATA_FILENAME]
    assert len(request_files) == 1

    # Verify metadata counts weren't incremented
    assert rq_client.metadata.total_request_count == 1
    assert rq_client.metadata.pending_request_count == 1


async def test_fetch_next_request(rq_client: FileSystemRequestQueueClient) -> None:
    """Test fetching the next request from the queue."""
    # Add requests
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(requests)

    # Fetch the first request
    first_request = await rq_client.fetch_next_request()
    assert first_request is not None
    assert first_request.url == 'https://example.com/1'

    # Check that it's marked as in-progress
    assert first_request.id in rq_client._in_progress

    # Fetch the second request
    second_request = await rq_client.fetch_next_request()
    assert second_request is not None
    assert second_request.url == 'https://example.com/2'

    # There should be no more requests
    empty_request = await rq_client.fetch_next_request()
    assert empty_request is None


async def test_fetch_forefront_requests(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that forefront requests are fetched first."""
    # Add regular requests
    await rq_client.add_batch_of_requests(
        [
            Request.from_url('https://example.com/regular1'),
            Request.from_url('https://example.com/regular2'),
        ]
    )

    # Add forefront requests
    await rq_client.add_batch_of_requests(
        [
            Request.from_url('https://example.com/priority1'),
            Request.from_url('https://example.com/priority2'),
        ],
        forefront=True,
    )

    # Fetch requests - they should come in priority order first
    next_request1 = await rq_client.fetch_next_request()
    assert next_request1 is not None
    assert next_request1.url.startswith('https://example.com/priority')

    next_request2 = await rq_client.fetch_next_request()
    assert next_request2 is not None
    assert next_request2.url.startswith('https://example.com/priority')

    next_request3 = await rq_client.fetch_next_request()
    assert next_request3 is not None
    assert next_request3.url.startswith('https://example.com/regular')

    next_request4 = await rq_client.fetch_next_request()
    assert next_request4 is not None
    assert next_request4.url.startswith('https://example.com/regular')


async def test_mark_request_as_handled(rq_client: FileSystemRequestQueueClient) -> None:
    """Test marking a request as handled."""
    # Add and fetch a request
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])
    request = await rq_client.fetch_next_request()
    assert request is not None

    # Mark it as handled
    result = await rq_client.mark_request_as_handled(request)
    assert result is not None
    assert result.was_already_handled is True

    # Verify it's no longer in-progress
    assert request.id not in rq_client._in_progress

    # Verify metadata was updated
    assert rq_client.metadata.handled_request_count == 1
    assert rq_client.metadata.pending_request_count == 0

    # Verify the file was updated with handled_at timestamp
    request_files = [f for f in rq_client.path_to_rq.glob('*.json') if f.name != METADATA_FILENAME]
    assert len(request_files) == 1

    with request_files[0].open() as f:
        content = json.load(f)
        assert 'handled_at' in content
        assert content['handled_at'] is not None


async def test_reclaim_request(rq_client: FileSystemRequestQueueClient) -> None:
    """Test reclaiming a request that failed processing."""
    # Add and fetch a request
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])
    request = await rq_client.fetch_next_request()
    assert request is not None

    # Reclaim the request
    result = await rq_client.reclaim_request(request)
    assert result is not None
    assert result.was_already_handled is False

    # Verify it's no longer in-progress
    assert request.id not in rq_client._in_progress

    # Should be able to fetch it again
    reclaimed_request = await rq_client.fetch_next_request()
    assert reclaimed_request is not None
    assert reclaimed_request.id == request.id


async def test_reclaim_request_with_forefront(rq_client: FileSystemRequestQueueClient) -> None:
    """Test reclaiming a request with forefront priority."""
    # Add requests
    await rq_client.add_batch_of_requests(
        [
            Request.from_url('https://example.com/first'),
            Request.from_url('https://example.com/second'),
        ]
    )

    # Fetch the first request
    first_request = await rq_client.fetch_next_request()
    assert first_request is not None
    assert first_request.url == 'https://example.com/first'

    # Reclaim it with forefront priority
    await rq_client.reclaim_request(first_request, forefront=True)

    # It should be returned before the second request
    reclaimed_request = await rq_client.fetch_next_request()
    assert reclaimed_request is not None
    assert reclaimed_request.url == 'https://example.com/first'


async def test_is_empty(rq_client: FileSystemRequestQueueClient) -> None:
    """Test checking if a queue is empty."""
    # Queue should start empty
    assert await rq_client.is_empty() is True

    # Add a request
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])
    assert await rq_client.is_empty() is False

    # Fetch and handle the request
    request = await rq_client.fetch_next_request()
    assert request is not None
    await rq_client.mark_request_as_handled(request)

    # Queue should be empty again
    assert await rq_client.is_empty() is True


async def test_get_request(rq_client: FileSystemRequestQueueClient) -> None:
    """Test getting a request by ID."""
    # Add a request
    response = await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])
    request_id = response.processed_requests[0].id

    # Get the request by ID
    request = await rq_client.get_request(request_id)
    assert request is not None
    assert request.id == request_id
    assert request.url == 'https://example.com'

    # Try to get a non-existent request
    not_found = await rq_client.get_request('non-existent-id')
    assert not_found is None


async def test_drop(configuration: Configuration) -> None:
    """Test dropping the queue removes files from the filesystem."""
    client = await FileSystemStorageClient().create_rq_client(
        name='drop_test',
        configuration=configuration,
    )

    # Add requests to create files
    await client.add_batch_of_requests(
        [
            Request.from_url('https://example.com/1'),
            Request.from_url('https://example.com/2'),
        ]
    )

    # Verify the directory exists
    rq_path = client.path_to_rq
    assert rq_path.exists()

    # Drop the client
    await client.drop()

    # Verify the directory was removed
    assert not rq_path.exists()


async def test_file_persistence(configuration: Configuration) -> None:
    """Test that requests are persisted to files and can be recovered after a 'restart'."""
    # Explicitly set purge_on_start to False to ensure files aren't deleted
    configuration.purge_on_start = False

    # Create a client and add requests
    client1 = await FileSystemStorageClient().create_rq_client(
        name='persistence_test',
        configuration=configuration,
    )

    await client1.add_batch_of_requests(
        [
            Request.from_url('https://example.com/1'),
            Request.from_url('https://example.com/2'),
        ]
    )

    # Fetch and handle one request
    request = await client1.fetch_next_request()
    assert request is not None
    await client1.mark_request_as_handled(request)

    # Get the storage directory path before clearing the cache
    storage_path = client1.path_to_rq
    assert storage_path.exists(), 'Request queue directory should exist'

    # Verify files exist
    request_files = list(storage_path.glob('*.json'))
    assert len(request_files) > 0, 'Request files should exist'

    # Create a new client with same name (which will load from files)
    client2 = await FileSystemStorageClient().create_rq_client(
        name='persistence_test',
        configuration=configuration,
    )

    # Verify state was recovered
    assert client2.metadata.total_request_count == 2
    assert client2.metadata.handled_request_count == 1
    assert client2.metadata.pending_request_count == 1

    # Should be able to fetch the remaining request
    remaining_request = await client2.fetch_next_request()
    assert remaining_request is not None
    assert remaining_request.url == 'https://example.com/2'

    # Clean up
    await client2.drop()


async def test_metadata_updates(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that metadata timestamps are updated correctly after operations."""
    # Record initial timestamps
    initial_created = rq_client.metadata.created_at
    initial_accessed = rq_client.metadata.accessed_at
    initial_modified = rq_client.metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates accessed_at
    await rq_client.is_empty()

    # Verify timestamps
    assert rq_client.metadata.created_at == initial_created
    assert rq_client.metadata.accessed_at > initial_accessed
    assert rq_client.metadata.modified_at == initial_modified

    accessed_after_get = rq_client.metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates modified_at
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify timestamps again
    assert rq_client.metadata.created_at == initial_created
    assert rq_client.metadata.modified_at > initial_modified
    assert rq_client.metadata.accessed_at > accessed_after_get
