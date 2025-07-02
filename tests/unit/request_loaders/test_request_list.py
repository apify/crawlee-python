from collections.abc import AsyncGenerator

from crawlee.request_loaders._request_list import RequestList


async def test_sync_traversal() -> None:
    request_list = RequestList(['https://placeholder.com', 'https://placeholder.io', 'https://placeholder.dev'])

    while not await request_list.is_finished():
        item = await request_list.fetch_next_request()
        assert item is not None

        await request_list.mark_request_as_handled(item)

    assert await request_list.is_empty()


async def test_async_traversal() -> None:
    async def generator() -> AsyncGenerator[str]:
        yield 'https://placeholder.com'
        yield 'https://placeholder.io'
        yield 'https://placeholder.dev'

    request_list = RequestList(generator())

    while not await request_list.is_finished():
        item = await request_list.fetch_next_request()
        assert item is not None

        await request_list.mark_request_as_handled(item)

    assert await request_list.is_empty()


async def test_is_empty_does_not_depend_on_fetch_next_request() -> None:
    request_list = RequestList(['https://placeholder.com', 'https://placeholder.io', 'https://placeholder.dev'])

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
