from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from unittest.mock import create_autospec

import pytest

from crawlee import Request
from crawlee.request_loaders import (
    FinishedStatus,
    ReadyStatus,
    RequestLoader,
    RequestManager,
    RequestManagerTandem,
    RequestSourceStatus,
    StalledStatus,
    WaitingStatus,
)
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
    """Test that the tandem serves requests from both the loader and the manager."""
    request_queue = await RequestQueue.open()

    if test_input.request_manager_items:
        await request_queue.add_requests(test_input.request_manager_items)

    mock_request_loader = create_autospec(RequestLoader, instance=True, spec_set=True)
    mock_request_loader.fetch_next_request.side_effect = lambda: test_input.request_loader_items.pop(0)
    mock_request_loader.check_readiness.side_effect = lambda: (
        ReadyStatus() if test_input.request_loader_items else FinishedStatus()
    )

    tandem = RequestManagerTandem(mock_request_loader, request_queue)
    processed = set[str]()

    while not isinstance(await tandem.check_readiness(), FinishedStatus):
        request = await tandem.fetch_next_request()
        assert request is not None
        processed.add(request.url)

        for new_request in test_input.discovered_items:
            await tandem.add_request(new_request)

        await tandem.mark_request_as_handled(request)

    assert processed == test_input.expected_result


READY_AT = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ('loader_status', 'manager_status', 'expected'),
    [
        pytest.param(ReadyStatus(), FinishedStatus(), ReadyStatus(), id='ready loader'),
        pytest.param(
            FinishedStatus(), WaitingStatus(ready_at=READY_AT), WaitingStatus(ready_at=READY_AT), id='waiting'
        ),
        pytest.param(WaitingStatus(), StalledStatus('stuck'), WaitingStatus(), id='waiting masks stalled'),
        pytest.param(FinishedStatus(), StalledStatus('stuck'), StalledStatus('stuck'), id='stalled'),
        pytest.param(FinishedStatus(), FinishedStatus(), FinishedStatus(), id='finished'),
    ],
)
async def test_readiness_joins_sources(
    loader_status: RequestSourceStatus,
    manager_status: RequestSourceStatus,
    expected: RequestSourceStatus,
) -> None:
    """Test that the tandem reports the loader and the manager as one source."""
    loader = create_autospec(RequestLoader, instance=True, spec_set=True)
    loader.check_readiness.return_value = loader_status
    manager = create_autospec(RequestManager, instance=True, spec_set=True)
    manager.check_readiness.return_value = manager_status

    assert await RequestManagerTandem(loader, manager).check_readiness() == expected


async def test_ready_loader_skips_manager() -> None:
    """Test that a ready loader answers without probing the manager."""
    loader = create_autospec(RequestLoader, instance=True, spec_set=True)
    loader.check_readiness.return_value = ReadyStatus()
    manager = create_autospec(RequestManager, instance=True, spec_set=True)

    assert await RequestManagerTandem(loader, manager).check_readiness() == ReadyStatus()
    manager.check_readiness.assert_not_awaited()


async def test_transfers_only_when_loader_ready() -> None:
    """Test that a loader that isn't ready doesn't block the manager's requests."""
    loader = create_autospec(RequestLoader, instance=True, spec_set=True)
    loader.check_readiness.return_value = WaitingStatus()
    manager = create_autospec(RequestManager, instance=True, spec_set=True)
    manager.fetch_next_request.return_value = Request.from_url('https://a.placeholder.com')

    request = await RequestManagerTandem(loader, manager).fetch_next_request()

    assert request is not None
    assert request.url == 'https://a.placeholder.com'
    loader.fetch_next_request.assert_not_awaited()
