from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import update_wrapper
from typing import Any
from unittest.mock import AsyncMock

import pytest

from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData


@pytest.fixture
def listener() -> AsyncMock:
    async def async_listener(payload: Any) -> None:
        pass

    al = AsyncMock()
    update_wrapper(al, async_listener)
    return al


async def test_emit_system_info_event(listener: AsyncMock) -> None:
    async with LocalEventManager(system_info_interval=timedelta(milliseconds=50)) as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await asyncio.sleep(0.2)

    assert listener.call_count >= 1
    assert isinstance(listener.call_args[0][0], EventSystemInfoData)
