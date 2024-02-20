from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee import Config
from crawlee.events import LocalEventManager
from crawlee.events.types import Event, EventSystemInfoData


@pytest.fixture()
def emit_system_info_event() -> AsyncMock:
    esie = AsyncMock()
    esie.__name__ = 'emit_system_info_event'  # To avoid issues with the function name
    return esie


@pytest.fixture()
def config() -> Config:
    return Config(
        system_info_interval=timedelta(milliseconds=20),
        max_used_cpu_ratio=0.8,
    )


@pytest.fixture()
def listener() -> AsyncMock:
    al = AsyncMock()
    al.__name__ = 'listener'  # To avoid issues with the function name
    return al


@pytest.mark.asyncio()
async def test_system_info_event_emitted_periodically(
    monkeypatch: pytest.MonkeyPatch,
    emit_system_info_event: AsyncMock,
    config: MagicMock,
) -> None:
    monkeypatch.setattr(LocalEventManager, '_emit_system_info_event', emit_system_info_event)

    async with LocalEventManager(config=config) as _:
        await asyncio.sleep(0.05)

        # Ensure the system info event was emitted at least twice
        assert emit_system_info_event.call_count >= 2


@pytest.mark.asyncio()
async def test_emit_system_info_event_invokes_registered_listeners(config: Config, listener: AsyncMock) -> None:
    async with LocalEventManager(config=config) as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await asyncio.sleep(0.15)

    assert listener.call_count >= 1
    assert isinstance(listener.call_args[0][0], EventSystemInfoData)


@pytest.mark.asyncio()
async def test_get_current_mem_usage_returns_positive_integer(config: Config) -> None:
    event_manager = LocalEventManager(config=config)
    mem_usage = event_manager._get_current_mem_usage()  # noqa: SLF001
    assert 0 < mem_usage < 1024 * 1024 * 1024 * 1024


@pytest.mark.asyncio()
async def test_get_cpu_info_returns_valid_load_ratio_info(config: Config) -> None:
    event_manager = LocalEventManager(config=config)
    cpu_info = await event_manager._get_cpu_info()  # noqa: SLF001
    assert 0 <= cpu_info.actual_ratio <= 1
