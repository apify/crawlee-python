from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from crawlee.events import LocalEventManager
from crawlee.events.types import Event, EventSystemInfoData

pytestmark = pytest.mark.asyncio()


@pytest.fixture()
def listener() -> AsyncMock:
    al = AsyncMock()
    al.__name__ = 'listener'  # To avoid issues with the function name
    return al


async def test_emit_system_info_event(listener: AsyncMock) -> None:
    async with LocalEventManager(system_info_interval=timedelta(milliseconds=50)) as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await asyncio.sleep(0.2)

    assert listener.call_count >= 1
    assert isinstance(listener.call_args[0][0], EventSystemInfoData)


async def test_get_memory_info_returns_valid_values() -> None:
    event_manager = LocalEventManager(system_info_interval=timedelta(milliseconds=50))
    memory_info = event_manager._get_memory_info()  # noqa: SLF001

    assert 0 < memory_info.used_bytes < memory_info.total_bytes
    assert memory_info.available_bytes + memory_info.used_bytes < memory_info.total_bytes
    assert memory_info.current_process_bytes + memory_info.child_processes_bytes < memory_info.used_bytes


async def test_get_cpu_info_returns_valid_values() -> None:
    event_manager = LocalEventManager(system_info_interval=timedelta(milliseconds=50))
    cpu_info = await event_manager._get_cpu_info()  # noqa: SLF001
    assert 0 <= cpu_info.current_usage_ratio <= 1
