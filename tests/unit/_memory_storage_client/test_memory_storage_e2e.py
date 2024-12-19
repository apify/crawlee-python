from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

import pytest

from crawlee import Request, service_locator
from crawlee.storages._key_value_store import KeyValueStore
from crawlee.storages._request_queue import RequestQueue


@pytest.mark.parametrize('purge_on_start', [True, False])
async def test_actor_memory_storage_client_key_value_store_e2e(
    monkeypatch: pytest.MonkeyPatch,
    purge_on_start: bool,  # noqa: FBT001
    prepare_test_env: Callable[[], None],
) -> None:
    """This test simulates two clean runs using memory storage.
    The second run attempts to access data created by the first one.
    We run 2 configurations with different `purge_on_start`."""
    # Configure purging env var
    monkeypatch.setenv('CRAWLEE_PURGE_ON_START', f'{int(purge_on_start)}')
    # Store old storage client so we have the object reference for comparison
    old_client = service_locator.get_storage_client()

    old_default_kvs = await KeyValueStore.open()
    old_non_default_kvs = await KeyValueStore.open(name='non-default')
    # Create data in default and non-default key-value store
    await old_default_kvs.set_value('test', 'default value')
    await old_non_default_kvs.set_value('test', 'non-default value')

    # We simulate another clean run, we expect the memory storage to read from the local data directory
    # Default storages are purged based on purge_on_start parameter.
    prepare_test_env()

    # Check if we're using a different memory storage instance
    assert old_client is not service_locator.get_storage_client()
    default_kvs = await KeyValueStore.open()
    assert default_kvs is not old_default_kvs
    non_default_kvs = await KeyValueStore.open(name='non-default')
    assert non_default_kvs is not old_non_default_kvs
    default_value = await default_kvs.get_value('test')

    if purge_on_start:
        assert default_value is None
    else:
        assert default_value == 'default value'

    assert await non_default_kvs.get_value('test') == 'non-default value'


@pytest.mark.parametrize('purge_on_start', [True, False])
async def test_actor_memory_storage_client_request_queue_e2e(
    monkeypatch: pytest.MonkeyPatch,
    purge_on_start: bool,  # noqa: FBT001
    prepare_test_env: Callable[[], None],
) -> None:
    """This test simulates two clean runs using memory storage.
    The second run attempts to access data created by the first one.
    We run 2 configurations with different `purge_on_start`."""
    # Configure purging env var
    monkeypatch.setenv('CRAWLEE_PURGE_ON_START', f'{int(purge_on_start)}')

    # Add some requests to the default queue
    default_queue = await RequestQueue.open()
    for i in range(6):
        # [0, 3] <- nothing special
        # [1, 4] <- forefront=True
        # [2, 5] <- handled=True
        request_url = f'http://example.com/{i}'
        forefront = i % 3 == 1
        was_handled = i % 3 == 2
        await default_queue.add_request(
            Request.from_url(
                unique_key=str(i),
                url=request_url,
                handled_at=datetime.now(timezone.utc) if was_handled else None,
                payload=b'test',
            ),
            forefront=forefront,
        )

    # We simulate another clean run, we expect the memory storage to read from the local data directory
    # Default storages are purged based on purge_on_start parameter.
    prepare_test_env()

    # Add some more requests to the default queue
    default_queue = await RequestQueue.open()
    for i in range(6, 12):
        # [6,  9] <- nothing special
        # [7, 10] <- forefront=True
        # [8, 11] <- handled=True
        request_url = f'http://example.com/{i}'
        forefront = i % 3 == 1
        was_handled = i % 3 == 2
        await default_queue.add_request(
            Request.from_url(
                unique_key=str(i),
                url=request_url,
                handled_at=datetime.now(timezone.utc) if was_handled else None,
                payload=b'test',
            ),
            forefront=forefront,
        )

    queue_info = await default_queue.get_info()
    assert queue_info is not None

    # If the queue was purged between the runs, only the requests from the second run should be present,
    # in the right order
    if purge_on_start:
        assert queue_info.total_request_count == 6
        assert queue_info.handled_request_count == 2

        expected_pending_request_order = [10, 7, 6, 9]
    # If the queue was NOT purged between the runs, all the requests should be in the queue in the right order
    else:
        assert queue_info.total_request_count == 12
        assert queue_info.handled_request_count == 4

        expected_pending_request_order = [10, 7, 4, 1, 0, 3, 6, 9]

    actual_requests = list[Request]()
    while req := await default_queue.fetch_next_request():
        actual_requests.append(req)

    assert [int(req.unique_key) for req in actual_requests] == expected_pending_request_order
    assert [req.url for req in actual_requests] == [f'http://example.com/{req.unique_key}' for req in actual_requests]
    assert [req.payload for req in actual_requests] == [b'test' for _ in actual_requests]
