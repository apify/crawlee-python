from collections.abc import AsyncGenerator

from crawlee.request_loaders._request_list import RequestList
from crawlee.storages import KeyValueStore


async def test_sync_traversal() -> None:
    request_list = RequestList(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    while not await request_list.is_finished():
        item = await request_list.fetch_next_request()
        assert item is not None

        await request_list.mark_request_as_handled(item)

    assert await request_list.is_empty()


async def test_async_traversal() -> None:
    async def generator() -> AsyncGenerator[str]:
        yield 'https://a.placeholder.com'
        yield 'https://b.placeholder.com'
        yield 'https://c.placeholder.com'

    request_list = RequestList(generator())

    while not await request_list.is_finished():
        item = await request_list.fetch_next_request()
        assert item is not None

        await request_list.mark_request_as_handled(item)

    assert await request_list.is_empty()


async def test_is_empty_does_not_depend_on_fetch_next_request() -> None:
    request_list = RequestList(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    item_1 = await request_list.fetch_next_request()
    assert item_1 is not None
    assert not await request_list.is_finished()

    item_2 = await request_list.fetch_next_request()
    assert item_2 is not None
    assert not await request_list.is_finished()

    item_3 = await request_list.fetch_next_request()
    assert item_3 is not None
    assert not await request_list.is_finished()

    assert await request_list.is_empty()
    assert not await request_list.is_finished()

    await request_list.mark_request_as_handled(item_1)
    await request_list.mark_request_as_handled(item_2)
    await request_list.mark_request_as_handled(item_3)

    assert await request_list.is_empty()
    assert await request_list.is_finished()


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


async def test_persist_requests_key_with_empty_iterator() -> None:
    """Test behavior when persist_requests_key is provided but the iterator is empty."""
    persist_key = 'test_empty_iterator'

    # Create request list with empty iterator
    request_list = RequestList([], persist_requests_key=persist_key)

    # Should be empty immediately
    assert await request_list.is_empty()
    assert await request_list.is_finished()

    # Check that empty requests were persisted
    kvs = await KeyValueStore.open()
    persisted_data = await kvs.get_value(persist_key)
    assert persisted_data is not None


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
