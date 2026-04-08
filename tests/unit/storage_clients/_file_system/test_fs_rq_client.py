from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from crawlee import Request, service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_clients._file_system import FileSystemRequestQueueClient


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    return Configuration(
        storage_dir=str(tmp_path),
    )


@pytest.fixture
async def rq_client() -> AsyncGenerator[FileSystemRequestQueueClient, None]:
    """A fixture for a file system request queue client."""
    client = await FileSystemStorageClient().create_rq_client(
        name='test-request-queue',
    )
    yield client
    await client.drop()


async def test_file_and_directory_creation() -> None:
    """Test that file system RQ creates proper files and directories."""
    client = await FileSystemStorageClient().create_rq_client(name='new-request-queue')

    # Verify files were created
    assert client.path_to_rq.exists()
    assert client.path_to_metadata.exists()

    # Verify metadata file structure
    with client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == (await client.get_metadata()).id
        assert metadata['name'] == 'new-request-queue'

    await client.drop()


async def test_request_file_persistence(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that requests are properly persisted to files."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_client.add_batch_of_requests(requests)

    # Verify request files are created
    request_files = list(rq_client.path_to_rq.glob('*.json'))
    # Should have 3 request files + 1 metadata file
    assert len(request_files) == 4
    assert rq_client.path_to_metadata in request_files

    # Verify actual request file content
    data_files = [f for f in request_files if f != rq_client.path_to_metadata]
    assert len(data_files) == 3

    for req_file in data_files:
        with req_file.open() as f:
            request_data = json.load(f)
            assert 'url' in request_data
            assert request_data['url'].startswith('https://example.com/')


async def test_opening_rq_does_not_have_side_effect_on_service_locator(configuration: Configuration) -> None:
    """Opening request queue client should cause setting storage client in the global service locator."""
    await FileSystemStorageClient().create_rq_client(name='test_request_queue', configuration=configuration)

    # Set some specific storage client in the service locator. There should be no `ServiceConflictError`.
    service_locator.set_storage_client(MemoryStorageClient())


async def test_drop_removes_directory(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that drop removes the entire RQ directory from disk."""
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    rq_path = rq_client.path_to_rq
    assert rq_path.exists()

    # Drop the request queue
    await rq_client.drop()

    assert not rq_path.exists()


async def test_metadata_file_updates(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that metadata file is updated correctly after operations."""
    # Record initial timestamps
    metadata = await rq_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await rq_client.is_empty()

    # Verify accessed timestamp was updated
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify modified timestamp was updated
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read

    # Verify metadata file is updated on disk
    with rq_client.path_to_metadata.open() as f:
        metadata_json = json.load(f)
        assert metadata_json['total_request_count'] == 1


async def test_data_persistence_across_reopens() -> None:
    """Test that requests persist correctly when reopening the same RQ."""
    storage_client = FileSystemStorageClient()

    # Create RQ and add requests
    original_client = await storage_client.create_rq_client(
        name='persistence-test',
    )

    test_requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
    ]
    await original_client.add_batch_of_requests(test_requests)

    rq_id = (await original_client.get_metadata()).id

    # Reopen by ID and verify requests persist
    reopened_client = await storage_client.create_rq_client(
        id=rq_id,
    )

    metadata = await reopened_client.get_metadata()
    assert metadata.total_request_count == 2

    # Fetch requests to verify they're still there
    request1 = await reopened_client.fetch_next_request()
    request2 = await reopened_client.fetch_next_request()

    assert request1 is not None
    assert request2 is not None
    assert {request1.url, request2.url} == {'https://example.com/1', 'https://example.com/2'}

    await reopened_client.drop()


async def test_in_progress_requests_recovered_after_crash() -> None:
    """Test that requests left in-progress at crash time are recovered as pending on reopen.

    Simulates a crash: requests are added, one is fetched (in-progress), state is persisted,
    then the queue is reopened. The in-progress request should be available for fetching again.
    """
    storage_client = FileSystemStorageClient()

    # Create RQ and add requests.
    original_client = await storage_client.create_rq_client(name='crash-recovery-test')

    test_requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]
    await original_client.add_batch_of_requests(test_requests)

    # Fetch one request, putting it in-progress (simulating work before crash).
    fetched = await original_client.fetch_next_request()
    assert fetched is not None

    # Persist state explicitly (simulating what happens periodically or at crash boundary).
    await original_client._state.persist_state()

    rq_id = (await original_client.get_metadata()).id

    # Simulate crash: reopen the queue without calling mark_request_as_handled or reclaim_request.
    reopened_client = await storage_client.create_rq_client(id=rq_id)

    # All 3 requests should be fetchable (the in-progress one should have been reclaimed).
    fetched_urls = set()
    for _ in range(3):
        req = await reopened_client.fetch_next_request()
        assert req is not None, f'Expected 3 fetchable requests, only got {len(fetched_urls)}'
        fetched_urls.add(req.url)

    assert fetched_urls == {'https://example.com/1', 'https://example.com/2', 'https://example.com/3'}

    # No more requests should be available.
    assert await reopened_client.fetch_next_request() is None

    await reopened_client.drop()


async def test_get_request_does_not_mark_in_progress(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that get_request does not block a request from being fetched."""
    request = Request.from_url('https://example.com/blocked')
    await rq_client.add_batch_of_requests([request])

    fetched = await rq_client.get_request(request.unique_key)
    assert fetched is not None
    assert fetched.unique_key == request.unique_key

    next_request = await rq_client.fetch_next_request()
    assert next_request is not None
    assert next_request.unique_key == request.unique_key


async def test_is_empty_cache_stale_after_full_lifecycle(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that the is_empty cache stays correct through add -> fetch -> handle -> add cycle.

    This exercises the scenario where a queue becomes empty, then new requests arrive.
    The cache must be invalidated so the crawler doesn't shut down.
    """
    # Add and fully process a request.
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_client.fetch_next_request()
    assert request is not None
    await rq_client.mark_request_as_handled(request)

    # Queue is now empty.
    assert await rq_client.is_empty() is True

    # Add a new request - cache must be invalidated.
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com/2')])

    # Must not return the stale cached True.
    assert await rq_client.is_empty() is False


async def test_is_empty_with_interleaved_operations(rq_client: FileSystemRequestQueueClient) -> None:
    """Test is_empty correctness with interleaved add, fetch, reclaim, and handle operations."""
    await rq_client.add_batch_of_requests(
        [
            Request.from_url('https://example.com/1'),
            Request.from_url('https://example.com/2'),
        ]
    )
    assert await rq_client.is_empty() is False

    # Fetch first request.
    req1 = await rq_client.fetch_next_request()
    assert req1 is not None
    assert await rq_client.is_empty() is False

    # Reclaim it (put back in queue).
    await rq_client.reclaim_request(req1)
    assert await rq_client.is_empty() is False

    # Fetch and handle both requests.
    for _ in range(2):
        req = await rq_client.fetch_next_request()
        assert req is not None
        assert await rq_client.is_empty() is False
        await rq_client.mark_request_as_handled(req)

    # Now should be empty.
    assert await rq_client.is_empty() is True


async def test_is_empty_no_stale_true_during_concurrent_add(rq_client: FileSystemRequestQueueClient) -> None:
    """Test that is_empty never returns a stale True while requests are being added.

    Uses an asyncio.Event to deterministically ensure is_empty contends on the lock
    while add_batch_of_requests is mid-operation.
    """
    assert await rq_client.is_empty() is True

    add_holding_lock = asyncio.Event()
    original_update_metadata = rq_client._update_metadata

    async def slow_update_metadata(**kwargs: Any) -> None:
        add_holding_lock.set()
        await asyncio.sleep(0)
        await original_update_metadata(**kwargs)

    async def check_empty_after_add_starts() -> bool:
        await add_holding_lock.wait()
        return await rq_client.is_empty()

    with patch.object(rq_client, '_update_metadata', side_effect=slow_update_metadata):
        _, is_empty_result = await asyncio.gather(
            rq_client.add_batch_of_requests([Request.from_url('https://example.com/race')]),
            check_empty_after_add_starts(),
        )

    assert is_empty_result is False
