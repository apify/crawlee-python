from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock, patch

import pytest

from crawlee import service_locator
from crawlee.autoscaling import ConcurrencySystem
from crawlee.autoscaling._snapshotter import Snapshotter
from crawlee.autoscaling._system_status import SystemStatus
from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator


@pytest.fixture
def system_status() -> Mock:
    """The system load verdicts the test hands to the concurrency system. Unconfigured, it reports an idle system."""
    return Mock(spec=SystemStatus)


@pytest.fixture
def make_concurrency_system(system_status: Mock) -> Iterator[type[ConcurrencySystem]]:
    """A `ConcurrencySystem` factory with the load measurement replaced by `system_status`."""
    with (
        patch('crawlee.autoscaling._concurrency_system.Snapshotter', return_value=AsyncMock(spec=Snapshotter)),
        patch('crawlee.autoscaling._concurrency_system.SystemStatus', return_value=system_status),
    ):
        yield ConcurrencySystem


@pytest.fixture
async def event_manager() -> AsyncGenerator[LocalEventManager, None]:
    """A global event manager that emits `SYSTEM_INFO` events only when the test does."""
    # The long interval keeps periodic events out of the test, and the first automatic event is consumed before
    # yielding.
    event_manager = LocalEventManager(system_info_interval=timedelta(hours=9999))

    initial_system_info_consumed = asyncio.Event()

    async def consume_automatic_system_info(_: EventSystemInfoData) -> None:
        initial_system_info_consumed.set()

    event_manager.on(event=Event.SYSTEM_INFO, listener=consume_automatic_system_info)

    async with event_manager:
        await initial_system_info_consumed.wait()
        event_manager.off(event=Event.SYSTEM_INFO, listener=consume_automatic_system_info)

        service_locator.set_event_manager(event_manager)
        yield event_manager
