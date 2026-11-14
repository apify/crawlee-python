from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee.statistics import Statistics


@pytest.mark.parametrize(
    'error',
    [
        pytest.param(RuntimeError('initialize failed'), id='failed'),
        pytest.param(asyncio.CancelledError(), id='cancelled'),
    ],
)
async def test_failed_enter_frees_stats(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    """Statistics whose entry fails or is cancelled stay inactive and can be entered again."""
    statistics = Statistics.with_default_state()
    monkeypatch.setattr(statistics._state, 'initialize', AsyncMock(side_effect=error))

    with pytest.raises(type(error)):
        async with statistics:
            pass

    assert not statistics.active
    monkeypatch.undo()

    async with statistics:
        assert statistics.active


async def test_failed_exit_frees_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Statistics whose exit fails are no longer active, so they can be entered again."""
    statistics = Statistics.with_default_state()

    with pytest.raises(RuntimeError, match='teardown failed'):
        async with statistics:
            monkeypatch.setattr(statistics._state, 'teardown', AsyncMock(side_effect=RuntimeError('teardown failed')))

    assert not statistics.active
    monkeypatch.undo()

    async with statistics:
        assert statistics.active


async def test_enter_during_exit_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Statistics can't be entered again until their exit has torn the state down."""
    statistics = Statistics.with_default_state()
    teardown_started = asyncio.Event()
    release_teardown = asyncio.Event()

    async def wait_for_release() -> None:
        teardown_started.set()
        await release_teardown.wait()

    await statistics.__aenter__()
    monkeypatch.setattr(statistics._state, 'teardown', AsyncMock(side_effect=wait_for_release))
    exit_task = asyncio.create_task(statistics.__aexit__(None, None, None))
    await teardown_started.wait()

    with pytest.raises(RuntimeError, match='already active'):
        async with statistics:
            pass

    release_teardown.set()
    await exit_task


async def test_failed_start_tears_down_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Statistics whose entry fails after loading the state tear the state down and can be entered again."""
    statistics = Statistics.with_default_state()
    teardown = AsyncMock(wraps=statistics._state.teardown)
    monkeypatch.setattr(statistics._state, 'teardown', teardown)
    monkeypatch.setattr(statistics._periodic_logger, 'start', Mock(side_effect=RuntimeError('start failed')))

    with pytest.raises(RuntimeError, match='start failed'):
        async with statistics:
            pass

    assert not statistics.active
    teardown.assert_awaited_once()
    monkeypatch.undo()

    async with statistics:
        assert statistics.active


async def test_cancelled_exit_finishes_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Statistics whose exit is cancelled still record when the run finished and tear their state down."""
    statistics = Statistics.with_default_state()

    with pytest.raises(asyncio.CancelledError):
        async with statistics:
            monkeypatch.setattr(statistics._periodic_logger, 'stop', AsyncMock(side_effect=asyncio.CancelledError()))

    assert statistics.state.crawler_finished_at is not None
    assert statistics._state.is_initialized is False
    monkeypatch.undo()
    await statistics._periodic_logger.stop()
