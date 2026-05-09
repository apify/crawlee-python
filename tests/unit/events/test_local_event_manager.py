from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData


async def test_emit_system_info_event() -> None:
    mocked_listener = AsyncMock()
    received = asyncio.Event()

    async def async_listener(payload: Any) -> None:
        await mocked_listener(payload)
        received.set()

    system_info_interval = timedelta(milliseconds=50)
    async with LocalEventManager(system_info_interval=system_info_interval) as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
        # Wait until the listener is invoked at least once. A generous timeout avoids flakiness on
        # loaded CI runners, where `psutil.cpu_percent(interval=0.1)` in `asyncio.to_thread` can
        # delay the first emission well beyond the configured interval.
        await asyncio.wait_for(received.wait(), timeout=10)

    assert mocked_listener.call_count >= 1
    assert isinstance(mocked_listener.call_args[0][0], EventSystemInfoData)
