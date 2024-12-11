from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import create_autospec

import pytest

from crawlee import Request
from crawlee.request_sources import RequestSource, RequestSourceTandem
from crawlee.storages import RequestQueue


@dataclass
class TestInput:
    request_source_items: list[str | Request | None]
    request_queue_items: list[str | Request]
    discovered_items: list[Request]
    expected_result: set[str]


@pytest.mark.parametrize(
    argnames=['test_input'],
    ids=[
        'basic_usage',
        'wait_for_read_only_source',
    ],
    argvalues=[
        TestInput(
            request_source_items=['http://a.com', 'http://b.com'],
            request_queue_items=[],
            discovered_items=[Request.from_url('http://c.com')],
            expected_result={
                'http://a.com',
                'http://b.com',
                'http://c.com',
            },
        ),
        TestInput(
            request_source_items=[Request.from_url('http://a.com'), None, Request.from_url('http://c.com')],
            request_queue_items=['http://b.com', 'http://d.com'],
            discovered_items=[],
            expected_result={
                'http://a.com',
                'http://b.com',
                'http://c.com',
                'http://d.com',
            },
        ),
    ],
)
async def test_basic_functionality(test_input: TestInput) -> None:
    request_queue = await RequestQueue.open()

    if test_input.request_queue_items:
        await request_queue.add_requests_batched(test_input.request_queue_items)

    mock_request_source = create_autospec(RequestSource, instance=True, spec_set=True)
    mock_request_source.fetch_next_request.side_effect = lambda: test_input.request_source_items.pop(0)
    mock_request_source.is_finished.side_effect = lambda: len(test_input.request_source_items) == 0

    tandem = RequestSourceTandem(mock_request_source, request_queue)
    processed = set[str]()

    while not await tandem.is_finished():
        request = await tandem.fetch_next_request()
        assert request is not None
        processed.add(request.url)

        for new_request in test_input.discovered_items:
            await tandem.add_request(new_request)

        await tandem.mark_request_as_handled(request)

    assert processed == test_input.expected_result
