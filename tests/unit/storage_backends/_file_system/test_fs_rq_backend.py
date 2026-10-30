from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from crawlee import Request, service_locator
from crawlee.configuration import Configuration
from crawlee.storage_backends import FileSystemStorageBackend, MemoryStorageBackend
from crawlee.storage_backends.models import RequestQueueMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_backends._file_system import FileSystemRequestQueueBackend


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    return Configuration(
        storage_dir=str(tmp_path),
    )


@pytest.fixture
async def rq_backend() -> AsyncGenerator[FileSystemRequestQueueBackend, None]:
    """A fixture for a file system request queue backend."""
    backend = await FileSystemStorageBackend().create_rq_backend(
        name='test-request-queue',
    )
    yield backend
    await backend.drop()


async def test_file_and_directory_creation() -> None:
    """Test that file system RQ creates proper files and directories."""
    backend = await FileSystemStorageBackend().create_rq_backend(name='new-request-queue')

    # Verify files were created
    assert backend.path_to_rq.exists()
    assert backend.path_to_metadata.exists()

    # Verify metadata file structure
    with backend.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == (await backend.get_metadata()).id
        assert metadata['name'] == 'new-request-queue'

    await backend.drop()


@pytest.mark.parametrize(
    'invalid_value',
    [
        pytest.param('../outside', id='parent-ref'),
        pytest.param('..', id='bare-parent'),
        pytest.param('/abs/outside', id='absolute-path'),
    ],
)
async def test_open_rejects_invalid_name_or_alias(
    configuration: Configuration, tmp_path: Path, invalid_value: str
) -> None:
    """The low-level backend must reject names/aliases that resolve outside the storage directory.

    This covers direct usage of the storage backend, which bypasses the high-level validation.
    """
    storage_backend = FileSystemStorageBackend()

    with pytest.raises(ValueError, match='Invalid storage name or alias'):
        await storage_backend.create_rq_backend(alias=invalid_value, configuration=configuration)

    with pytest.raises(ValueError, match='Invalid storage name or alias'):
        await storage_backend.create_rq_backend(name=invalid_value, configuration=configuration)

    # Nothing should have been written outside the configured storage directory.
    assert not (tmp_path / 'outside').exists()


async def test_request_file_persistence(rq_backend: FileSystemRequestQueueBackend) -> None:
    """Test that requests are properly persisted to files."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_backend.add_batch_of_requests(requests)

    # Verify request files are created
    request_files = list(rq_backend.path_to_rq.glob('*.json'))
    # Should have 3 request files + 1 metadata file
    assert len(request_files) == 4
    assert rq_backend.path_to_metadata in request_files

    # Verify actual request file content
    data_files = [f for f in request_files if f != rq_backend.path_to_metadata]
    assert len(data_files) == 3

    for req_file in data_files:
        with req_file.open() as f:
            request_data = json.load(f)
            assert 'url' in request_data
            assert request_data['url'].startswith('https://example.com/')
            assert 'uniqueKey' in request_data


async def test_opening_rq_does_not_have_side_effect_on_service_locator(configuration: Configuration) -> None:
    """Opening request queue backend should cause setting storage backend in the global service locator."""
    await FileSystemStorageBackend().create_rq_backend(name='test_request_queue', configuration=configuration)

    # Set some specific storage backend in the service locator. There should be no `ServiceConflictError`.
    service_locator.set_storage_backend(MemoryStorageBackend())


async def test_drop_removes_directory(rq_backend: FileSystemRequestQueueBackend) -> None:
    """Test that drop removes the entire RQ directory from disk."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com')])

    rq_path = rq_backend.path_to_rq
    assert rq_path.exists()

    # Drop the request queue
    await rq_backend.drop()

    assert not rq_path.exists()


async def test_metadata_file_updates(rq_backend: FileSystemRequestQueueBackend) -> None:
    """Test that metadata file is updated correctly after operations."""
    # Record initial timestamps
    metadata = await rq_backend.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await rq_backend.is_empty()

    # Verify accessed timestamp was updated
    metadata = await rq_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify modified timestamp was updated
    metadata = await rq_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read

    # Verify metadata file is updated on disk
    with rq_backend.path_to_metadata.open() as f:
        metadata_json = json.load(f)
        assert metadata_json['totalRequestCount'] == 1


async def test_data_persistence_across_reopens() -> None:
    """Test that requests persist correctly when reopening the same RQ."""
    storage_backend = FileSystemStorageBackend()

    # Create RQ and add requests
    original_backend = await storage_backend.create_rq_backend(
        name='persistence-test',
    )

    test_requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
    ]
    await original_backend.add_batch_of_requests(test_requests)

    rq_id = (await original_backend.get_metadata()).id

    # Reopen by ID and verify requests persist
    reopened_backend = await storage_backend.create_rq_backend(
        id=rq_id,
    )

    metadata = await reopened_backend.get_metadata()
    assert metadata.total_request_count == 2

    # Fetch requests to verify they're still there
    request1 = await reopened_backend.fetch_next_request()
    request2 = await reopened_backend.fetch_next_request()

    assert request1 is not None
    assert request2 is not None
    assert {request1.url, request2.url} == {'https://example.com/1', 'https://example.com/2'}

    await reopened_backend.drop()


async def test_snake_case_files_from_v1_are_still_read() -> None:
    """Metadata and request files with snake_case keys, as Crawlee v1 wrote them, still load."""
    storage_backend = FileSystemStorageBackend()
    backend = await storage_backend.create_rq_backend(name='snake-case-rq')
    request = Request.from_url('https://example.com/1', label='detail')
    request.crawl_depth = 2
    await backend.add_batch_of_requests([request])
    rq_id = (await backend.get_metadata()).id

    # Rewrite every file the way v1 serialized it, without aliases.
    for path in backend.path_to_rq.glob('*.json'):
        data = json.loads(path.read_text())
        model = RequestQueueMetadata if path == backend.path_to_metadata else Request
        snake_case_data = model.model_validate(data).model_dump(mode='json', by_alias=False)
        path.write_text(json.dumps(snake_case_data))

    reopened_backend = await storage_backend.create_rq_backend(id=rq_id)
    assert (await reopened_backend.get_metadata()).total_request_count == 1

    fetched = await reopened_backend.fetch_next_request()
    assert fetched is not None
    assert fetched.unique_key == request.unique_key
    assert fetched.label == 'detail'
    assert fetched.crawl_depth == 2

    await reopened_backend.drop()


async def test_in_progress_requests_recovered_after_crash() -> None:
    """Test that requests left in-progress at crash time are recovered as pending on reopen.

    Simulates a crash: requests are added, one is fetched (in-progress), state is persisted,
    then the queue is reopened. The in-progress request should be available for fetching again.
    """
    storage_backend = FileSystemStorageBackend()

    # Create RQ and add requests.
    original_backend = await storage_backend.create_rq_backend(name='crash-recovery-test')

    test_requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]
    await original_backend.add_batch_of_requests(test_requests)

    # Fetch one request, putting it in-progress (simulating work before crash).
    fetched = await original_backend.fetch_next_request()
    assert fetched is not None

    # Persist state explicitly (simulating what happens periodically or at crash boundary).
    await original_backend._state.persist_state()

    rq_id = (await original_backend.get_metadata()).id

    # Simulate crash: reopen the queue without calling mark_request_as_handled or reclaim_request.
    reopened_backend = await storage_backend.create_rq_backend(id=rq_id)

    # All 3 requests should be fetchable (the in-progress one should have been reclaimed).
    fetched_urls = set()
    for _ in range(3):
        req = await reopened_backend.fetch_next_request()
        assert req is not None, f'Expected 3 fetchable requests, only got {len(fetched_urls)}'
        fetched_urls.add(req.url)

    assert fetched_urls == {'https://example.com/1', 'https://example.com/2', 'https://example.com/3'}

    # No more requests should be available.
    assert await reopened_backend.fetch_next_request() is None

    await reopened_backend.drop()


async def test_get_request_does_not_mark_in_progress(rq_backend: FileSystemRequestQueueBackend) -> None:
    """Test that get_request does not block a request from being fetched."""
    request = Request.from_url('https://example.com/blocked')
    await rq_backend.add_batch_of_requests([request])

    fetched = await rq_backend.get_request(request.unique_key)
    assert fetched is not None
    assert fetched.unique_key == request.unique_key

    next_request = await rq_backend.fetch_next_request()
    assert next_request is not None
    assert next_request.unique_key == request.unique_key


async def test_forefront_add_fetch_handle_parses_linear_number_of_files(
    rq_backend: FileSystemRequestQueueBackend,
) -> None:
    """Test that per-request forefront adds (the `RequestManagerTandem` pattern) do not rescan all request files."""
    with patch.object(rq_backend, '_parse_request_file', wraps=rq_backend._parse_request_file) as parse_spy:
        n = 25
        for i in range(n):
            await rq_backend.add_batch_of_requests([Request.from_url(f'https://example.com/{i}')], forefront=True)
            request = await rq_backend.fetch_next_request()
            assert request is not None
            await rq_backend.mark_request_as_handled(request)

    # Before the fix, each forefront add invalidated the cache and each fetch re-parsed every request file
    # ever written, giving n * (n + 1) / 2 parses in total. With the fix, new forefront requests go straight
    # to the cache, so only the initial refresh parses anything.
    assert parse_spy.call_count <= 5


async def test_cache_refresh_skips_handled_request_files(
    rq_backend: FileSystemRequestQueueBackend,
) -> None:
    """Test that a cache refresh reads only pending request files, not files of already handled requests."""
    await rq_backend.add_batch_of_requests([Request.from_url(f'https://example.com/{i}') for i in range(5)])
    for _ in range(5):
        request = await rq_backend.fetch_next_request()
        assert request is not None
        await rq_backend.mark_request_as_handled(request)

    # The 5 handled request files stay on disk. Add 5 new requests; fetching the next one refreshes
    # the cache, which must not parse the handled files again.
    await rq_backend.add_batch_of_requests([Request.from_url(f'https://example.com/new/{i}') for i in range(5)])

    with patch.object(rq_backend, '_parse_request_file', wraps=rq_backend._parse_request_file) as parse_spy:
        request = await rq_backend.fetch_next_request()

    assert request is not None
    assert parse_spy.call_count == 5


async def test_handled_requests_pruned_from_pending_state(rq_backend: FileSystemRequestQueueBackend) -> None:
    """Test that handling a request removes it from the pending state mappings so the state stays bounded."""
    await rq_backend.add_batch_of_requests([Request.from_url(f'https://example.com/{i}') for i in range(3)])

    handled_keys = set()
    for _ in range(3):
        request = await rq_backend.fetch_next_request()
        assert request is not None
        await rq_backend.mark_request_as_handled(request)
        handled_keys.add(request.unique_key)

    state = rq_backend._state.current_value
    assert not state.regular_requests
    assert not state.forefront_requests
    assert not state.in_progress_requests
    assert state.handled_requests == handled_keys


async def test_handled_requests_deduplicated_after_reopen() -> None:
    """Test that requests handled before a reopen are not served again and still deduplicate re-adds."""
    storage_backend = FileSystemStorageBackend()
    backend = await storage_backend.create_rq_backend(name='handled-reopen-test')

    request = Request.from_url('https://example.com/handled')
    await backend.add_batch_of_requests([request])
    fetched = await backend.fetch_next_request()
    assert fetched is not None
    await backend.mark_request_as_handled(fetched)
    await backend._state.persist_state()

    rq_id = (await backend.get_metadata()).id
    reopened = await storage_backend.create_rq_backend(id=rq_id)

    response = await reopened.add_batch_of_requests([request])
    assert response.processed_requests[0].was_already_handled is True
    assert await reopened.fetch_next_request() is None

    await reopened.drop()
