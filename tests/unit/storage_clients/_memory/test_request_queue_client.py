from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee._request import RequestState

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storage_clients._memory import RequestQueueClient


@pytest.fixture
async def request_queue_client(memory_storage_client: MemoryStorageClient) -> RequestQueueClient:
    request_queues_client = memory_storage_client.request_queues()
    rq_info = await request_queues_client.get_or_create(name='test')
    return memory_storage_client.request_queue(rq_info.id)


async def test_nonexistent(memory_storage_client: MemoryStorageClient) -> None:
    request_queue_client = memory_storage_client.request_queue(id='nonexistent-id')
    assert await request_queue_client.get() is None
    with pytest.raises(ValueError, match='Request queue with id "nonexistent-id" does not exist.'):
        await request_queue_client.update(name='test-update')
    await request_queue_client.delete()


async def test_get(request_queue_client: RequestQueueClient) -> None:
    await asyncio.sleep(0.1)
    info = await request_queue_client.get()
    assert info is not None
    assert info.id == request_queue_client.id
    assert info.accessed_at != info.created_at


async def test_update(request_queue_client: RequestQueueClient) -> None:
    new_rq_name = 'test-update'
    request = Request.from_url('https://apify.com')
    await request_queue_client.add_request(request)
    old_rq_info = await request_queue_client.get()
    assert old_rq_info is not None
    assert old_rq_info.name is not None
    old_rq_directory = Path(
        request_queue_client._memory_storage_client.request_queues_directory,
        old_rq_info.name,
    )
    new_rq_directory = Path(request_queue_client._memory_storage_client.request_queues_directory, new_rq_name)
    assert (old_rq_directory / 'fvwscO2UJLdr10B.json').exists() is True
    assert (new_rq_directory / 'fvwscO2UJLdr10B.json').exists() is False

    await asyncio.sleep(0.1)
    updated_rq_info = await request_queue_client.update(name=new_rq_name)
    assert (old_rq_directory / 'fvwscO2UJLdr10B.json').exists() is False
    assert (new_rq_directory / 'fvwscO2UJLdr10B.json').exists() is True
    # Only modified_at and accessed_at should be different
    assert old_rq_info.created_at == updated_rq_info.created_at
    assert old_rq_info.modified_at != updated_rq_info.modified_at
    assert old_rq_info.accessed_at != updated_rq_info.accessed_at

    # Should fail with the same name
    with pytest.raises(ValueError, match='Request queue with name "test-update" already exists'):
        await request_queue_client.update(name=new_rq_name)


async def test_delete(request_queue_client: RequestQueueClient) -> None:
    await request_queue_client.add_request(Request.from_url('https://apify.com'))
    rq_info = await request_queue_client.get()
    assert rq_info is not None

    rq_directory = Path(request_queue_client._memory_storage_client.request_queues_directory, str(rq_info.name))
    assert (rq_directory / 'fvwscO2UJLdr10B.json').exists() is True

    await request_queue_client.delete()
    assert (rq_directory / 'fvwscO2UJLdr10B.json').exists() is False

    # Does not crash when called again
    await request_queue_client.delete()


async def test_list_head(request_queue_client: RequestQueueClient) -> None:
    await request_queue_client.add_request(Request.from_url('https://apify.com'))
    await request_queue_client.add_request(Request.from_url('https://example.com'))
    list_head = await request_queue_client.list_head()
    assert len(list_head.items) == 2

    for item in list_head.items:
        assert item.id is not None


async def test_request_state_serialization(request_queue_client: RequestQueueClient) -> None:
    request = Request.from_url('https://crawlee.dev', payload=b'test')
    request.state = RequestState.UNPROCESSED

    await request_queue_client.add_request(request)

    result = await request_queue_client.list_head()
    assert len(result.items) == 1
    assert result.items[0] == request

    got_request = await request_queue_client.get_request(request.id)

    assert request == got_request


async def test_add_record(request_queue_client: RequestQueueClient) -> None:
    processed_request_forefront = await request_queue_client.add_request(
        Request.from_url('https://apify.com'),
        forefront=True,
    )
    processed_request_not_forefront = await request_queue_client.add_request(
        Request.from_url('https://example.com'),
        forefront=False,
    )

    assert processed_request_forefront.id is not None
    assert processed_request_not_forefront.id is not None
    assert processed_request_forefront.was_already_handled is False
    assert processed_request_not_forefront.was_already_handled is False

    rq_info = await request_queue_client.get()
    assert rq_info is not None
    assert rq_info.pending_request_count == rq_info.total_request_count == 2
    assert rq_info.handled_request_count == 0


async def test_get_record(request_queue_client: RequestQueueClient) -> None:
    request_url = 'https://apify.com'
    processed_request = await request_queue_client.add_request(Request.from_url(request_url))

    request = await request_queue_client.get_request(processed_request.id)
    assert request is not None
    assert request.url == request_url

    # Non-existent id
    assert (await request_queue_client.get_request('non-existent id')) is None


async def test_update_record(request_queue_client: RequestQueueClient) -> None:
    processed_request = await request_queue_client.add_request(Request.from_url('https://apify.com'))
    request = await request_queue_client.get_request(processed_request.id)
    assert request is not None

    rq_info_before_update = await request_queue_client.get()
    assert rq_info_before_update is not None
    assert rq_info_before_update.pending_request_count == 1
    assert rq_info_before_update.handled_request_count == 0

    request.handled_at = datetime.now(timezone.utc)
    request_update_info = await request_queue_client.update_request(request)

    assert request_update_info.was_already_handled is False

    rq_info_after_update = await request_queue_client.get()
    assert rq_info_after_update is not None
    assert rq_info_after_update.pending_request_count == 0
    assert rq_info_after_update.handled_request_count == 1


async def test_delete_record(request_queue_client: RequestQueueClient) -> None:
    processed_request_pending = await request_queue_client.add_request(
        Request.from_url(
            url='https://apify.com',
            unique_key='pending',
        ),
    )

    processed_request_handled = await request_queue_client.add_request(
        Request.from_url(
            url='https://apify.com',
            unique_key='handled',
            handled_at=datetime.now(timezone.utc),
        ),
    )

    rq_info_before_delete = await request_queue_client.get()
    assert rq_info_before_delete is not None
    assert rq_info_before_delete.pending_request_count == 1

    await request_queue_client.delete_request(processed_request_pending.id)
    rq_info_after_first_delete = await request_queue_client.get()
    assert rq_info_after_first_delete is not None
    assert rq_info_after_first_delete.pending_request_count == 0
    assert rq_info_after_first_delete.handled_request_count == 1

    await request_queue_client.delete_request(processed_request_handled.id)
    rq_info_after_second_delete = await request_queue_client.get()
    assert rq_info_after_second_delete is not None
    assert rq_info_after_second_delete.pending_request_count == 0
    assert rq_info_after_second_delete.handled_request_count == 0

    # Does not crash when called again
    await request_queue_client.delete_request(processed_request_pending.id)


async def test_forefront(request_queue_client: RequestQueueClient) -> None:
    # this should create a queue with requests in this order:
    # Handled:
    #     2, 5, 8
    # Not handled:
    #     7, 4, 1, 0, 3, 6
    for i in range(9):
        request_url = f'http://example.com/{i}'
        forefront = i % 3 == 1
        was_handled = i % 3 == 2
        await request_queue_client.add_request(
            Request.from_url(
                url=request_url,
                unique_key=str(i),
                handled_at=datetime.now(timezone.utc) if was_handled else None,
            ),
            forefront=forefront,
        )

    # Check that the queue head (unhandled items) is in the right order
    queue_head = await request_queue_client.list_head()
    req_unique_keys = [req.unique_key for req in queue_head.items]
    assert req_unique_keys == ['7', '4', '1', '0', '3', '6']

    # Mark request #1 as handled
    await request_queue_client.update_request(
        Request.from_url(
            url='http://example.com/1',
            unique_key='1',
            handled_at=datetime.now(timezone.utc),
        ),
    )
    # Move request #3 to forefront
    await request_queue_client.update_request(
        Request.from_url(url='http://example.com/3', unique_key='3'),
        forefront=True,
    )

    # Check that the queue head (unhandled items) is in the right order after the updates
    queue_head = await request_queue_client.list_head()
    req_unique_keys = [req.unique_key for req in queue_head.items]
    assert req_unique_keys == ['3', '7', '4', '0', '6']


async def test_add_duplicate_record(request_queue_client: RequestQueueClient) -> None:
    processed_request = await request_queue_client.add_request(Request.from_url('https://apify.com'))
    processed_request_duplicate = await request_queue_client.add_request(Request.from_url('https://apify.com'))

    assert processed_request.id == processed_request_duplicate.id
    assert processed_request_duplicate.was_already_present is True
