from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_clients._file_system import FileSystemRequestQueueClient


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


async def test_file_and_directory_creation(configuration: Configuration) -> None:
    """Test that file system RQ creates proper files and directories."""
    client = await FileSystemStorageClient().create_rq_client(
        name='new_request_queue',
        configuration=configuration,
    )

    # Verify files were created
    assert client.path_to_rq.exists()
    assert client.path_to_metadata.exists()

    # Verify metadata file structure
    with client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == (await client.get_metadata()).id
        assert metadata['name'] == 'new_request_queue'

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


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that requests persist correctly when reopening the same RQ."""
    storage_client = FileSystemStorageClient()

    # Create RQ and add requests
    original_client = await storage_client.create_rq_client(
        name='persistence-test',
        configuration=configuration,
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
        configuration=configuration,
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
