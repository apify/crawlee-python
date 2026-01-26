from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData


async def test_emit_system_info_event() -> None:
    mocked_listener = AsyncMock()

    async def async_listener(payload: Any) -> None:
        await mocked_listener(payload)

    system_info_interval = timedelta(milliseconds=50)
    test_tolerance_coefficient = 10
    async with LocalEventManager(system_info_interval=system_info_interval) as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
        await asyncio.sleep(system_info_interval.total_seconds() * test_tolerance_coefficient)

    assert mocked_listener.call_count >= 1
    assert isinstance(mocked_listener.call_args[0][0], EventSystemInfoData)
