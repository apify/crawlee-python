from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData


@pytest.fixture
def listener() -> AsyncMock:
    async def async_listener(payload: Any) -> None:
        pass

    return AsyncMock(target=async_listener)


async def test_emit_system_info_event(listener: AsyncMock) -> None:
    system_info_interval = timedelta(milliseconds=50)
    test_tolerance_coefficient = 10
    async with LocalEventManager(system_info_interval=system_info_interval) as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await asyncio.sleep(system_info_interval.total_seconds() * test_tolerance_coefficient)

    assert listener.call_count >= 1
    assert isinstance(listener.call_args[0][0], EventSystemInfoData)
