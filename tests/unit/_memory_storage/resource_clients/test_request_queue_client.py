from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from crawlee._memory_storage import MemoryStorageClient
    from crawlee._memory_storage.resource_clients import RequestQueueClient


@pytest.fixture()
async def request_queue_client(memory_storage_client: MemoryStorageClient) -> RequestQueueClient:
    request_queues_client = memory_storage_client.request_queues()
    rq_info = await request_queues_client.get_or_create(name='test')
    return memory_storage_client.request_queue(rq_info['id'])


async def test_nonexistent(memory_storage_client: MemoryStorageClient) -> None:
    request_queue_client = memory_storage_client.request_queue(request_queue_id='nonexistent-id')
    assert await request_queue_client.get() is None
    with pytest.raises(ValueError, match='Request queue with id "nonexistent-id" does not exist.'):
        await request_queue_client.update(name='test-update')
    await request_queue_client.delete()


async def test_get(request_queue_client: RequestQueueClient) -> None:
    await asyncio.sleep(0.1)
    info = await request_queue_client.get()
    assert info is not None
    assert info['id'] == request_queue_client._id
    assert info['accessedAt'] != info['createdAt']


async def test_update(request_queue_client: RequestQueueClient) -> None:
    new_rq_name = 'test-update'
    await request_queue_client.add_request(
        {
            'uniqueKey': 'https://apify.com',
            'url': 'https://apify.com',
        }
    )
    old_rq_info = await request_queue_client.get()
    assert old_rq_info is not None
    old_rq_directory = os.path.join(
        request_queue_client._memory_storage_client._request_queues_directory, old_rq_info['name']
    )
    new_rq_directory = os.path.join(request_queue_client._memory_storage_client._request_queues_directory, new_rq_name)
    assert os.path.exists(os.path.join(old_rq_directory, 'fvwscO2UJLdr10B.json')) is True
    assert os.path.exists(os.path.join(new_rq_directory, 'fvwscO2UJLdr10B.json')) is False

    await asyncio.sleep(0.1)
    updated_rq_info = await request_queue_client.update(name=new_rq_name)
    assert os.path.exists(os.path.join(old_rq_directory, 'fvwscO2UJLdr10B.json')) is False
    assert os.path.exists(os.path.join(new_rq_directory, 'fvwscO2UJLdr10B.json')) is True
    # Only modifiedAt and accessedAt should be different
    assert old_rq_info['createdAt'] == updated_rq_info['createdAt']
    assert old_rq_info['modifiedAt'] != updated_rq_info['modifiedAt']
    assert old_rq_info['accessedAt'] != updated_rq_info['accessedAt']

    # Should fail with the same name
    with pytest.raises(ValueError, match='Request queue with name "test-update" already exists'):
        await request_queue_client.update(name=new_rq_name)


async def test_delete(request_queue_client: RequestQueueClient) -> None:
    await request_queue_client.add_request(
        {
            'uniqueKey': 'https://apify.com',
            'url': 'https://apify.com',
        }
    )
    rq_info = await request_queue_client.get()
    assert rq_info is not None

    rq_directory = os.path.join(request_queue_client._memory_storage_client._request_queues_directory, rq_info['name'])
    assert os.path.exists(os.path.join(rq_directory, 'fvwscO2UJLdr10B.json')) is True

    await request_queue_client.delete()
    assert os.path.exists(os.path.join(rq_directory, 'fvwscO2UJLdr10B.json')) is False

    # Does not crash when called again
    await request_queue_client.delete()


async def test_list_head(request_queue_client: RequestQueueClient) -> None:
    request_1_url = 'https://apify.com'
    request_2_url = 'https://example.com'
    await request_queue_client.add_request(
        {
            'uniqueKey': request_1_url,
            'url': request_1_url,
        }
    )
    await request_queue_client.add_request(
        {
            'uniqueKey': request_2_url,
            'url': request_2_url,
        }
    )
    list_head = await request_queue_client.list_head()
    assert len(list_head['items']) == 2
    for item in list_head['items']:
        assert 'id' in item


async def test_add_record(request_queue_client: RequestQueueClient) -> None:
    request_forefront_url = 'https://apify.com'
    request_not_forefront_url = 'https://example.com'
    request_forefront_info = await request_queue_client.add_request(
        {
            'uniqueKey': request_forefront_url,
            'url': request_forefront_url,
        },
        forefront=True,
    )
    request_not_forefront_info = await request_queue_client.add_request(
        {
            'uniqueKey': request_not_forefront_url,
            'url': request_not_forefront_url,
        },
        forefront=False,
    )

    assert request_forefront_info.get('requestId') is not None
    assert request_not_forefront_info.get('requestId') is not None
    assert request_forefront_info['wasAlreadyHandled'] is False
    assert request_not_forefront_info['wasAlreadyHandled'] is False

    rq_info = await request_queue_client.get()
    assert rq_info is not None
    assert rq_info['pendingRequestCount'] == rq_info['totalRequestCount'] == 2
    assert rq_info['handledRequestCount'] == 0


async def test_get_record(request_queue_client: RequestQueueClient) -> None:
    request_url = 'https://apify.com'
    request_info = await request_queue_client.add_request(
        {
            'uniqueKey': request_url,
            'url': request_url,
        }
    )
    request = await request_queue_client.get_request(request_info['requestId'])
    assert request is not None
    assert 'id' in request
    assert request['url'] == request['uniqueKey'] == request_url

    # Non-existent id
    assert (await request_queue_client.get_request('non-existent id')) is None


async def test_update_record(request_queue_client: RequestQueueClient) -> None:
    request_url = 'https://apify.com'
    request_info = await request_queue_client.add_request(
        {
            'uniqueKey': request_url,
            'url': request_url,
        }
    )
    request = await request_queue_client.get_request(request_info['requestId'])
    assert request is not None

    rq_info_before_update = await request_queue_client.get()
    assert rq_info_before_update is not None
    assert rq_info_before_update['pendingRequestCount'] == 1
    assert rq_info_before_update['handledRequestCount'] == 0

    request_update_info = await request_queue_client.update_request(
        {**request, 'handledAt': datetime.now(timezone.utc)}
    )
    assert request_update_info['wasAlreadyHandled'] is False

    rq_info_after_update = await request_queue_client.get()
    assert rq_info_after_update is not None
    assert rq_info_after_update['pendingRequestCount'] == 0
    assert rq_info_after_update['handledRequestCount'] == 1


async def test_delete_record(request_queue_client: RequestQueueClient) -> None:
    request_url = 'https://apify.com'
    pending_request_info = await request_queue_client.add_request(
        {
            'uniqueKey': 'pending',
            'url': request_url,
        }
    )
    handled_request_info = await request_queue_client.add_request(
        {
            'uniqueKey': 'handled',
            'url': request_url,
            'handledAt': datetime.now(tz=timezone.utc),
        }
    )

    rq_info_before_delete = await request_queue_client.get()
    assert rq_info_before_delete is not None
    assert rq_info_before_delete['pendingRequestCount'] == 1
    assert rq_info_before_delete['pendingRequestCount'] == 1

    await request_queue_client.delete_request(pending_request_info['requestId'])
    rq_info_after_first_delete = await request_queue_client.get()
    assert rq_info_after_first_delete is not None
    assert rq_info_after_first_delete['pendingRequestCount'] == 0
    assert rq_info_after_first_delete['handledRequestCount'] == 1

    await request_queue_client.delete_request(handled_request_info['requestId'])
    rq_info_after_second_delete = await request_queue_client.get()
    assert rq_info_after_second_delete is not None
    assert rq_info_after_second_delete['pendingRequestCount'] == 0
    assert rq_info_after_second_delete['handledRequestCount'] == 0

    # Does not crash when called again
    await request_queue_client.delete_request(pending_request_info['requestId'])


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
            {
                'uniqueKey': str(i),
                'url': request_url,
                'handledAt': datetime.now(timezone.utc) if was_handled else None,
            },
            forefront=forefront,
        )

    # Check that the queue head (unhandled items) is in the right order
    queue_head = await request_queue_client.list_head()
    req_unique_keys = [req['uniqueKey'] for req in queue_head['items']]
    assert req_unique_keys == ['7', '4', '1', '0', '3', '6']

    # Mark request #1 as handled
    await request_queue_client.update_request(
        {
            'uniqueKey': '1',
            'url': 'http://example.com/1',
            'handledAt': datetime.now(timezone.utc),
        }
    )
    # Move request #3 to forefront
    await request_queue_client.update_request(
        {
            'uniqueKey': '3',
            'url': 'http://example.com/3',
        },
        forefront=True,
    )

    # Check that the queue head (unhandled items) is in the right order after the updates
    queue_head = await request_queue_client.list_head()
    req_unique_keys = [req['uniqueKey'] for req in queue_head['items']]
    assert req_unique_keys == ['3', '7', '4', '0', '6']
