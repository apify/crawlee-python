from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import create_autospec

import pytest

from crawlee import Request
from crawlee.request_loaders import RequestLoader, RequestManagerTandem
from crawlee.storages import RequestQueue


@dataclass
class TestInput:
    __test__ = False

    request_loader_items: list[str | Request | None]
    request_manager_items: list[str | Request]
    discovered_items: list[Request]
    expected_result: set[str]


@pytest.mark.parametrize(
    argnames='test_input',
    argvalues=[
        pytest.param(
            TestInput(
                request_loader_items=['https://a.placeholder.com', 'https://b.placeholder.com'],
                request_manager_items=[],
                discovered_items=[Request.from_url('https://c.placeholder.com')],
                expected_result={
                    'https://a.placeholder.com',
                    'https://b.placeholder.com',
                    'https://c.placeholder.com',
                },
            ),
            id='basic_usage',
        ),
        pytest.param(
            TestInput(
                request_loader_items=[
                    Request.from_url('https://a.placeholder.com'),
                    None,
                    Request.from_url('https://c.placeholder.com'),
                ],
                request_manager_items=['https://b.placeholder.com', 'http://d.com'],
                discovered_items=[],
                expected_result={
                    'https://a.placeholder.com',
                    'https://b.placeholder.com',
                    'https://c.placeholder.com',
                    'http://d.com',
                },
            ),
            id='wait_for_read_only_source',
        ),
    ],
)
async def test_basic_functionality(test_input: TestInput) -> None:
    request_queue = await RequestQueue.open()

    if test_input.request_manager_items:
        await request_queue.add_requests(test_input.request_manager_items)

    mock_request_loader = create_autospec(RequestLoader, instance=True, spec_set=True)
    mock_request_loader.fetch_next_request.side_effect = lambda: test_input.request_loader_items.pop(0)
    mock_request_loader.is_finished.side_effect = lambda: len(test_input.request_loader_items) == 0

    tandem = RequestManagerTandem(mock_request_loader, request_queue)
    processed = set[str]()

    while not await tandem.is_finished():
        request = await tandem.fetch_next_request()
        assert request is not None
        processed.add(request.url)

        for new_request in test_input.discovered_items:
            await tandem.add_request(new_request)

        await tandem.mark_request_as_handled(request)

    assert processed == test_input.expected_result
