from unittest.mock import create_autospec

from crawlee import Request
from crawlee.request_sources import RequestList, RequestSource, RequestSourceTandem
from crawlee.storages import RequestQueue


async def test_basic_usage() -> None:
    request_queue = await RequestQueue.open()
    request_list = RequestList(['http://a.com', 'http://b.com'])

    tandem = RequestSourceTandem(request_list, request_queue)
    processed = set[str]()

    while not await tandem.is_finished():
        request = await tandem.fetch_next_request()
        assert request is not None

        processed.add(request.url)
        await tandem.add_request(Request.from_url('http://c.com'))

        await tandem.mark_request_as_handled(request)

    assert processed == {
        'http://a.com',
        'http://b.com',
        'http://c.com',
    }

    assert await tandem.get_handled_count() == 3
    assert await request_list.get_handled_count() == 2
    assert await request_queue.get_handled_count() == 3


async def test_wait_for_read_only_source() -> None:
    request_queue = await RequestQueue.open()
    await request_queue.add_requests_batched(['http://b.com', 'http://d.com'])

    request_source_items = [Request.from_url('http://a.com'), None, Request.from_url('http://c.com')]

    mock_request_source = create_autospec(RequestSource, instance=True, spec_set=True)
    mock_request_source.fetch_next_request.side_effect = lambda: request_source_items.pop(0)
    mock_request_source.is_finished.side_effect = lambda: len(request_source_items) == 0

    tandem = RequestSourceTandem(mock_request_source, request_queue)
    processed = set[str]()

    while not await tandem.is_finished():
        request = await tandem.fetch_next_request()
        assert request is not None
        processed.add(request.url)
        await tandem.mark_request_as_handled(request)

    assert processed == {
        'http://a.com',
        'http://b.com',
        'http://c.com',
        'http://d.com',
    }
