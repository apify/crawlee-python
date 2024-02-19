from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee.autoscaling.types import LoadRatioInfo
from crawlee.events import LocalEventManager
from crawlee.events.types import Event, EventSystemInfoData


@pytest.fixture()
def emit_system_info_event() -> AsyncMock:
    esie = AsyncMock()
    esie.__name__ = 'emit_system_info_event'  # To avoid issues with the function name
    return esie


@pytest.fixture()
def config(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    cfg = MagicMock()
    monkeypatch.setattr(cfg, 'system_info_interval', timedelta(milliseconds=20))
    monkeypatch.setattr(cfg, 'max_used_cpu_ratio', 0.8)
    return cfg


@pytest.fixture()
def local_event_manager(config: MagicMock) -> LocalEventManager:
    return LocalEventManager(config=config)


@pytest.fixture()
def listener() -> AsyncMock:
    al = AsyncMock()
    al.__name__ = 'listener'  # To avoid issues with the function name
    return al


@pytest.mark.asyncio()
async def test_system_info_event_emitted_periodically(
    monkeypatch: pytest.MonkeyPatch,
    emit_system_info_event: AsyncMock,
    local_event_manager: LocalEventManager,
) -> None:
    monkeypatch.setattr(local_event_manager, '_emit_system_info_event', emit_system_info_event)
    await local_event_manager.__aenter__()
    await asyncio.sleep(0.05)

    # Ensure the system info event was emitted at least twice
    assert emit_system_info_event.call_count >= 2

    await local_event_manager.__aexit__(None, None, None)


@pytest.mark.asyncio()
async def test_get_current_mem_usage_returns_positive_integer(local_event_manager: LocalEventManager) -> None:
    mem_usage = local_event_manager._get_current_mem_usage()
    assert isinstance(mem_usage, int)
    assert mem_usage > 0


@pytest.mark.asyncio()
async def test_get_cpu_info_returns_valid_load_ratio_info(local_event_manager: LocalEventManager) -> None:
    cpu_info = await local_event_manager._get_cpu_info()
    assert isinstance(cpu_info, LoadRatioInfo)
    assert 0 <= cpu_info.actual_ratio <= 1


@pytest.mark.asyncio()
async def test_emit_system_info_event_invokes_registered_listeners(
    local_event_manager: LocalEventManager,
    listener: AsyncMock,
) -> None:
    local_event_manager.on(event=Event.SYSTEM_INFO, listener=listener)

    await local_event_manager._emit_system_info_event()
    await local_event_manager._wait_for_all_listeners_to_complete()

    assert listener.call_count == 1
    assert isinstance(listener.call_args[0][0], EventSystemInfoData)
