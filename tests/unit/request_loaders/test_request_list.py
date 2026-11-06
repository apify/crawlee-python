import asyncio
import logging
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest

from crawlee import service_locator
from crawlee.events import EventManager
from crawlee.events._types import Event, EventPersistStateData
from crawlee.request_loaders import RequestListState
from crawlee.request_loaders._request_list import RequestList
from crawlee.storages import KeyValueStore

URLS = ['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com']


async def test_sync_traversal() -> None:
    """Test that a list built from a sync iterable is traversed to the end."""
    request_list = RequestList(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    while (await request_list.check_readiness()).status != 'finished':
        item = await request_list.fetch_next_request()
        assert item is not None

        await request_list.mark_request_as_handled(item)

    assert await request_list.get_handled_count() == 3


async def test_async_traversal() -> None:
    """Test that a list built from an async generator is traversed to the end."""

    async def generator() -> AsyncGenerator[str]:
        yield 'https://a.placeholder.com'
        yield 'https://b.placeholder.com'
        yield 'https://c.placeholder.com'

    request_list = RequestList(generator())

    while (await request_list.check_readiness()).status != 'finished':
        item = await request_list.fetch_next_request()
        assert item is not None

        await request_list.mark_request_as_handled(item)

    assert await request_list.get_handled_count() == 3


async def test_pending_count_counts_fetched() -> None:
    """Test that the pending count covers only the fetched requests not handled yet, since the source is lazy."""
    request_list = RequestList(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    first = await request_list.fetch_next_request()
    second = await request_list.fetch_next_request()
    assert first is not None
    assert second is not None
    await request_list.mark_request_as_handled(first)

    assert await request_list.get_pending_count() == 1


async def test_readiness_during_traversal() -> None:
    """Test that the list waits for fetched requests and finishes once they're all handled."""
    request_list = RequestList(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    items = []
    for _ in range(3):
        assert (await request_list.check_readiness()).status == 'ready'
        item = await request_list.fetch_next_request()
        assert item is not None
        items.append(item)

    assert (await request_list.check_readiness()).status == 'waiting'

    for item in items:
        await request_list.mark_request_as_handled(item)

    assert (await request_list.check_readiness()).status == 'finished'


async def test_persist_requests_key_with_sync_iterable() -> None:
    """Test that persist_requests_key persists request data from a sync iterable."""
    persist_key = 'test_requests_persist_sync'
    urls = ['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com']

    # Create a request list with persistence enabled
    request_list = RequestList(urls, persist_requests_key=persist_key)

    # Fetch one request to trigger initialization
    first_request = await request_list.fetch_next_request()
    assert first_request is not None
    assert first_request.url == 'https://a.placeholder.com'

    # Check that the requests were persisted
    kvs = await KeyValueStore.open()
    persisted_data = await kvs.get_value(persist_key)
    assert persisted_data is not None
    assert [request['url'] for request in persisted_data['requests']] == urls


async def test_persist_requests_key_with_empty_iterator() -> None:
    """Test behavior when persist_requests_key is provided but the iterator is empty."""
    persist_key = 'test_empty_iterator'

    # Create request list with empty iterator
    request_list = RequestList([], persist_requests_key=persist_key)

    # Should be finished immediately
    assert (await request_list.check_readiness()).status == 'finished'

    # Check that empty requests were persisted
    kvs = await KeyValueStore.open()
    persisted_data = await kvs.get_value(persist_key)
    assert persisted_data is not None
    assert persisted_data['requests'] == []


async def test_requests_restoration_without_state() -> None:
    """Test that persisted request data is properly restored on subsequent RequestList creation."""
    persist_requests_key = 'test_requests_restoration'
    urls = ['https://restore1.placeholder.com', 'https://restore2.placeholder.com']

    # Create first request list and process one request
    request_list_1 = RequestList(urls, persist_requests_key=persist_requests_key)
    first_request = await request_list_1.fetch_next_request()
    assert first_request is not None
    assert first_request.url == 'https://restore1.placeholder.com'
    await request_list_1.mark_request_as_handled(first_request)

    # Create second request list with same persist key (simulating restart)
    # Since we don't have state persistence, it will start from the beginning of the persisted data
    spy = iter(['1', '2', '3'])
    request_list_2 = RequestList(spy, persist_requests_key=persist_requests_key)

    # Should be able to fetch requests from persisted data, but starts from beginning
    first_request_again = await request_list_2.fetch_next_request()
    assert first_request_again is not None
    assert first_request_again.url == 'https://restore1.placeholder.com'
    await request_list_2.mark_request_as_handled(first_request_again)

    # Make sure that the second instance did not consume the input iterator
    assert len(list(spy)) == 3


async def test_state_restoration() -> None:
    """Test that persisted processing state is properly restored on subsequent RequestList creation."""
    persist_state_key = 'test_state_restoration'
    urls = [
        'https://restore1.placeholder.com',
        'https://restore2.placeholder.com',
        'https://restore3.placeholder.com',
        'https://restore4.placeholder.com',
    ]

    # Create first request list and process one request
    request_list_1 = RequestList(
        urls,
        persist_state_key=persist_state_key,
    )

    first_request = await request_list_1.fetch_next_request()
    assert first_request is not None
    assert first_request.url == 'https://restore1.placeholder.com'
    await request_list_1.mark_request_as_handled(first_request)
    await request_list_1._state.persist_state()

    # Create second request list with same persist key (simulating restart)
    request_list_2 = RequestList(
        urls,
        persist_state_key=persist_state_key,
    )

    # Should be able to continue where the previous instance left off
    next_request = await request_list_2.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://restore2.placeholder.com'
    await request_list_2.mark_request_as_handled(next_request)

    next_request = await request_list_2.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://restore3.placeholder.com'
    await request_list_2.mark_request_as_handled(next_request)

    next_request = await request_list_2.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://restore4.placeholder.com'
    await request_list_2.mark_request_as_handled(next_request)


async def test_requests_and_state_restoration() -> None:
    """Test that persisted request data and processing state is properly restored on subsequent RequestList creation."""
    persist_requests_key = 'test_requests_restoration'
    persist_state_key = 'test_state_restoration'
    urls = [
        'https://restore1.placeholder.com',
        'https://restore2.placeholder.com',
        'https://restore3.placeholder.com',
    ]

    # Create first request list and process one request
    request_list_1 = RequestList(
        urls,
        persist_requests_key=persist_requests_key,
        persist_state_key=persist_state_key,
    )

    first_request = await request_list_1.fetch_next_request()
    assert first_request is not None
    assert first_request.url == 'https://restore1.placeholder.com'
    await request_list_1.mark_request_as_handled(first_request)
    await request_list_1._state.persist_state()

    # Create second request list with same persist key (simulating restart)
    spy = iter(['1', '2', '3'])
    request_list_2 = RequestList(
        spy,
        persist_requests_key=persist_requests_key,
        persist_state_key=persist_state_key,
    )

    # Should be able to fetch requests from persisted data and continue where the previous instance left off
    next_request = await request_list_2.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://restore2.placeholder.com'
    await request_list_2.mark_request_as_handled(next_request)

    next_request = await request_list_2.fetch_next_request()
    assert next_request is not None
    assert next_request.url == 'https://restore3.placeholder.com'
    await request_list_2.mark_request_as_handled(next_request)

    # Make sure that the second instance did not consume the input iterator
    assert len(list(spy)) == 3


async def test_persist_requests_key_only_persists_once() -> None:
    """Test that requests are only persisted once, even with multiple RequestList instances."""
    persist_key = 'test_requests_once'
    urls = ['https://once1.placeholder.com', 'https://once2.placeholder.com']

    # Create first request list
    request_list_1 = RequestList(urls, persist_requests_key=persist_key)
    await request_list_1.fetch_next_request()  # Trigger persistence

    # Get initial persisted data
    kvs = await KeyValueStore.open()
    initial_data = await kvs.get_value(persist_key)
    assert initial_data is not None

    # Create second request list with different data
    different_urls = ['https://different.placeholder.com']
    request_list_2 = RequestList(different_urls, persist_requests_key=persist_key)
    await request_list_2.fetch_next_request()  # Should use persisted data, not new data

    # Verify the persisted data hasn't changed
    current_data = await kvs.get_value(persist_key)
    assert current_data == initial_data

    # The request should come from the original persisted data, not the new iterator
    fetched_request = await request_list_2.fetch_next_request()
    assert fetched_request is not None
    assert fetched_request.url == 'https://once2.placeholder.com'  # From original data


async def test_handle_invalid_url() -> None:
    """Test that invalid URLs are handled gracefully."""
    request_list = RequestList(['invalid-url.com', 'https://valid.placeholder.com'])

    # First request is invalid, should be skipped without crashing
    request = await request_list.fetch_next_request()
    assert request is not None
    assert request.url == 'https://valid.placeholder.com'
    await request_list.mark_request_as_handled(request)


async def test_handle_invalid_url_with_persistence() -> None:
    """Test that invalid URLs are handled gracefully even when persistence is enabled."""
    persist_key = 'test_invalid_url_persistence'
    request_list = RequestList(['invalid-url.com', 'https://valid.placeholder.com'], persist_requests_key=persist_key)

    # First request is invalid, should be skipped without crashing
    request = await request_list.fetch_next_request()
    assert request is not None
    assert request.url == 'https://valid.placeholder.com'
    await request_list.mark_request_as_handled(request)

    # Check that the valid URL was persisted and the invalid one was not
    kvs = await KeyValueStore.open()
    persisted_data = await kvs.get_value(persist_key)
    assert persisted_data is not None
    assert [request['url'] for request in persisted_data['requests']] == ['https://valid.placeholder.com']


async def _drain(request_list: RequestList) -> list[str]:
    served = []
    while (request := await request_list.fetch_next_request()) is not None:
        served.append(request.url)
        await request_list.mark_request_as_handled(request)
    return served


async def _persist_position(position: RequestListState) -> None:
    kvs = await KeyValueStore.open()
    await kvs.set_value('state', position.model_dump(mode='json', by_alias=True))


@pytest.mark.parametrize(
    'persist_requests_key',
    [
        pytest.param(None, id='source'),
        pytest.param('requests', id='persisted-requests'),
    ],
)
async def test_restore_serves_in_progress_requests_again(persist_requests_key: str | None) -> None:
    """Requests in progress when the state was persisted are served again first, and the list then finishes."""
    first_list = RequestList(URLS, persist_state_key='state', persist_requests_key=persist_requests_key)
    first_request = await first_list.fetch_next_request()
    assert first_request is not None
    await first_list.fetch_next_request()
    await first_list.mark_request_as_handled(first_request)
    await first_list._state.persist_state()

    restored_list = RequestList(URLS, persist_state_key='state', persist_requests_key=persist_requests_key)

    assert await restored_list.get_handled_count() == 1
    assert await restored_list.get_pending_count() == 1
    assert await restored_list.get_total_count() == 2
    assert await _drain(restored_list) == ['https://b.placeholder.com', 'https://c.placeholder.com']
    assert (await restored_list.check_readiness()).status == 'finished'
    assert await restored_list.get_handled_count() == 3
    assert await restored_list.get_total_count() == 3


async def test_restore_reports_ready_while_only_in_progress_requests_remain() -> None:
    """A restored list with an exhausted source reports ready while in-progress requests are left to serve again."""
    await _persist_position(RequestListState(next_index=3, in_progress={'https://c.placeholder.com'}))
    request_list = RequestList(URLS, persist_state_key='state')

    assert (await request_list.check_readiness()).status == 'ready'
    assert await _drain(request_list) == ['https://c.placeholder.com']
    assert (await request_list.check_readiness()).status == 'finished'


async def test_restore_drops_in_progress_keys_not_fetched(caplog: pytest.LogCaptureFixture) -> None:
    """An in-progress key that isn't among the fetched requests is dropped with a warning instead of blocking."""
    position = RequestListState(
        next_index=1,
        next_unique_key='https://b.placeholder.com',
        in_progress={'https://a.placeholder.com', 'https://unknown.placeholder.com'},
    )
    await _persist_position(position)
    request_list = RequestList(URLS, persist_state_key='state')

    with caplog.at_level(logging.WARNING):
        assert await _drain(request_list) == URLS

    assert (await request_list.check_readiness()).status == 'finished'
    assert any(
        record.message.startswith("RequestList's in-progress field is not consistent") for record in caplog.records
    )


async def test_restore_skips_invalid_requests_before_position() -> None:
    """Restoring the position skips the fetched requests, not the invalid entries among them."""
    source = ['https://a.placeholder.com', 'invalid-url.com', 'https://b.placeholder.com', 'https://c.placeholder.com']
    first_list = RequestList(source, persist_state_key='state')
    for _ in range(2):
        request = await first_list.fetch_next_request()
        assert request is not None
        await first_list.mark_request_as_handled(request)
    await first_list._state.persist_state()

    restored_list = RequestList(source, persist_state_key='state')

    assert await _drain(restored_list) == ['https://c.placeholder.com']


async def test_mismatch_keeps_persisted_in_progress_keys() -> None:
    """A list that doesn't match the persisted position raises and leaves the persisted in-progress keys intact."""
    first_list = RequestList(URLS, persist_state_key='state')
    await first_list.fetch_next_request()
    await first_list._state.persist_state()

    changed_list = RequestList([URLS[1], URLS[0], URLS[2]], persist_state_key='state')
    with pytest.raises(RuntimeError, match='Mismatch at index 1'):
        await changed_list.check_readiness()
    await changed_list._state.persist_state()

    kvs = await KeyValueStore.open()
    assert (await kvs.get_value('state'))['inProgress'] == [URLS[0]]


async def test_mismatch_leaves_no_persisted_requests() -> None:
    """A position mismatch doesn't persist the requests, so the list restores once the source is right again."""
    first_list = RequestList(URLS, persist_state_key='state')
    request = await first_list.fetch_next_request()
    assert request is not None
    await first_list.mark_request_as_handled(request)
    await first_list._state.persist_state()
    reordered_urls = ['https://b.placeholder.com', 'https://a.placeholder.com', 'https://c.placeholder.com']
    mismatched_list = RequestList(reordered_urls, persist_state_key='state', persist_requests_key='requests')

    with pytest.raises(RuntimeError, match=r'Mismatch at index 1'):
        await mismatched_list.fetch_next_request()

    kvs = await KeyValueStore.open()
    assert await kvs.get_value('requests') is None
    restored_list = RequestList(URLS, persist_state_key='state', persist_requests_key='requests')
    assert await _drain(restored_list) == ['https://b.placeholder.com', 'https://c.placeholder.com']


async def test_concurrent_first_calls_restore_position_once() -> None:
    """Concurrent first calls restore the position once instead of advancing the source twice."""
    await _persist_position(RequestListState(next_index=1, next_unique_key='https://b.placeholder.com'))
    request_list = RequestList(URLS, persist_state_key='state')

    _, request = await asyncio.gather(request_list.check_readiness(), request_list.fetch_next_request())

    assert request is not None
    assert request.url == 'https://b.placeholder.com'


async def test_state_option_sets_start_position() -> None:
    """The `state` option sets the position to start from when there is no persisted state."""
    state = RequestListState(next_index=1, next_unique_key='https://b.placeholder.com')
    request_list = RequestList(URLS, state=state)

    assert await _drain(request_list) == ['https://b.placeholder.com', 'https://c.placeholder.com']


async def test_get_state_resumes_another_list() -> None:
    """`get_state` returns a copy that makes another request list resume from the same position."""
    first_list = RequestList(URLS)
    first_request = await first_list.fetch_next_request()
    assert first_request is not None
    await first_list.fetch_next_request()
    await first_list.mark_request_as_handled(first_request)

    copied_state = await first_list.get_state()
    copied_state.in_progress.clear()

    resumed_list = RequestList(URLS, state=await first_list.get_state())

    assert await _drain(resumed_list) == ['https://b.placeholder.com', 'https://c.placeholder.com']


async def test_persisted_state_wins_over_state_option() -> None:
    """A persisted state is restored instead of the one passed in the `state` option."""
    first_list = RequestList(URLS, persist_state_key='state')
    request = await first_list.fetch_next_request()
    assert request is not None
    await first_list.mark_request_as_handled(request)
    await first_list.persist_state()

    state = RequestListState(next_index=2, next_unique_key='https://c.placeholder.com')
    restored_list = RequestList(URLS, persist_state_key='state', state=state)

    assert await _drain(restored_list) == ['https://b.placeholder.com', 'https://c.placeholder.com']


async def test_persist_state_without_key_raises() -> None:
    """`persist_state` raises when the request list has no `persist_state_key`."""
    request_list = RequestList(URLS)

    with pytest.raises(RuntimeError, match=r'`persist_state_key` is not set'):
        await request_list.persist_state()


async def test_failed_persist_state_is_logged(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed write in `persist_state` is logged instead of raised."""
    request_list = RequestList(URLS, persist_state_key='state')
    await request_list.fetch_next_request()
    kvs = await KeyValueStore.open()
    monkeypatch.setattr(kvs, 'set_value', AsyncMock(side_effect=RuntimeError('store is on fire')))

    with caplog.at_level(logging.ERROR):
        await request_list.persist_state()

    assert any(record.message == 'Attempted to persist state, but failed.' for record in caplog.records)


async def test_close_persists_state_and_stops_listening() -> None:
    """Leaving the context persists the state once more, and later PERSIST_STATE events no longer persist it."""
    async with EventManager() as event_manager:
        service_locator.set_event_manager(event_manager)
        kvs = await KeyValueStore.open()

        async with RequestList(URLS, persist_state_key='state') as request_list:
            request = await request_list.fetch_next_request()
            assert request is not None
            await request_list.mark_request_as_handled(request)

        assert (await kvs.get_value('state'))['nextIndex'] == 1

        await request_list.fetch_next_request()
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

        assert (await kvs.get_value('state'))['nextIndex'] == 1


async def test_close_before_first_use_stops_listening() -> None:
    """A list closed before its first use doesn't persist on PERSIST_STATE events once it's used."""
    async with EventManager() as event_manager:
        service_locator.set_event_manager(event_manager)
        kvs = await KeyValueStore.open()
        request_list = RequestList(URLS, persist_state_key='state')
        await request_list.close()

        await request_list.fetch_next_request()
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

        assert (await kvs.get_value('state'))['nextIndex'] == 0


async def test_close_before_failed_first_use_stops_listening(monkeypatch: pytest.MonkeyPatch) -> None:
    """A list closed before a first use that fails doesn't persist on PERSIST_STATE events."""
    async with EventManager() as event_manager:
        service_locator.set_event_manager(event_manager)
        kvs = await KeyValueStore.open()
        state = RequestListState(next_index=1, next_unique_key='https://unknown.placeholder.com')
        request_list = RequestList(URLS, persist_state_key='state', state=state)
        await request_list.close()

        with pytest.raises(RuntimeError, match='Mismatch at index 1'):
            await request_list.fetch_next_request()

        set_value = AsyncMock()
        monkeypatch.setattr(kvs, 'set_value', set_value)
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

        set_value.assert_not_called()
