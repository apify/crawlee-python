from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from crawlee.browsers import BrowserPool


async def test_timed_out_plugin_is_not_exited() -> None:
    """A pool whose plugin entry timed out exits only the plugins it entered, not that one or the ones after it."""
    entered_plugin = AsyncMock()
    slow_plugin = AsyncMock()
    slow_plugin.__aenter__.side_effect = asyncio.Event().wait
    later_plugin = AsyncMock()
    browser_pool = BrowserPool(
        plugins=[entered_plugin, slow_plugin, later_plugin], operation_timeout=timedelta(milliseconds=10)
    )

    async with browser_pool:
        pass

    assert not browser_pool.active
    entered_plugin.__aexit__.assert_awaited_once()
    slow_plugin.__aexit__.assert_not_awaited()
    later_plugin.__aexit__.assert_not_awaited()


@pytest.mark.parametrize(
    'error',
    [
        pytest.param(RuntimeError('plugin failed'), id='failed'),
        pytest.param(asyncio.CancelledError(), id='cancelled'),
    ],
)
async def test_failed_enter_frees_pool(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    """A pool whose entry fails or is cancelled stops its tasks and entered plugins and can be entered again."""
    entered_plugin = AsyncMock()
    failing_plugin = AsyncMock()
    failing_plugin.__aenter__.side_effect = error
    browser_pool = BrowserPool(plugins=[entered_plugin, failing_plugin])
    identify_stop = AsyncMock(wraps=browser_pool._identify_inactive_browsers_task.stop)
    close_stop = AsyncMock(wraps=browser_pool._close_inactive_browsers_task.stop)
    monkeypatch.setattr(browser_pool._identify_inactive_browsers_task, 'stop', identify_stop)
    monkeypatch.setattr(browser_pool._close_inactive_browsers_task, 'stop', close_stop)

    with pytest.raises(type(error)):
        async with browser_pool:
            pass

    assert not browser_pool.active
    identify_stop.assert_awaited_once()
    close_stop.assert_awaited_once()
    entered_plugin.__aexit__.assert_awaited_once()
    failing_plugin.__aexit__.assert_not_awaited()

    failing_plugin.__aenter__.side_effect = None
    async with browser_pool:
        assert browser_pool.active

    assert entered_plugin.__aexit__.await_count == 2
    failing_plugin.__aexit__.assert_awaited_once()


async def test_failed_rollback_keeps_enter_error(caplog: pytest.LogCaptureFixture) -> None:
    """A plugin that fails to exit while the pool rolls back a failed entry is logged and keeps the entry error."""
    entered_plugin = AsyncMock()
    entered_plugin.__aexit__.side_effect = RuntimeError('exit failed')
    failing_plugin = AsyncMock()
    failing_plugin.__aenter__.side_effect = RuntimeError('enter failed')
    browser_pool = BrowserPool(plugins=[entered_plugin, failing_plugin])

    with pytest.raises(RuntimeError, match='enter failed'):
        async with browser_pool:
            pass

    assert not browser_pool.active
    entered_plugin.__aexit__.assert_awaited_once()
    assert 'Exiting a browser plugin failed while rolling back the pool entry.' in caplog.text


async def test_cancelled_rollback_exits_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pool whose entry rollback is cancelled while stopping its tasks still exits the plugins it entered."""
    entered_plugin = AsyncMock()
    failing_plugin = AsyncMock()
    failing_plugin.__aenter__.side_effect = RuntimeError('enter failed')
    browser_pool = BrowserPool(plugins=[entered_plugin, failing_plugin])
    monkeypatch.setattr(
        browser_pool._identify_inactive_browsers_task, 'stop', AsyncMock(side_effect=asyncio.CancelledError())
    )

    with pytest.raises(asyncio.CancelledError):
        async with browser_pool:
            pass

    assert not browser_pool.active
    entered_plugin.__aexit__.assert_awaited_once()
    monkeypatch.undo()
    await browser_pool._identify_inactive_browsers_task.stop()


async def test_failed_exit_frees_pool() -> None:
    """A pool whose plugin fails to exit is no longer active and can be entered again."""
    plugin = AsyncMock()
    plugin.__aexit__.side_effect = RuntimeError('plugin failed')
    browser_pool = BrowserPool(plugins=[plugin])

    with pytest.raises(RuntimeError, match='plugin failed'):
        async with browser_pool:
            pass

    assert not browser_pool.active

    plugin.__aexit__.side_effect = None
    async with browser_pool:
        assert browser_pool.active

    assert plugin.__aexit__.await_count == 2


async def test_enter_during_exit_raises() -> None:
    """A pool can't be entered again until its exit has exited its plugins."""
    exit_started = asyncio.Event()
    release_exit = asyncio.Event()

    async def wait_for_release(*_: object) -> None:
        exit_started.set()
        await release_exit.wait()

    plugin = AsyncMock()
    plugin.__aexit__.side_effect = wait_for_release
    browser_pool = BrowserPool(plugins=[plugin])
    await browser_pool.__aenter__()
    exit_task = asyncio.create_task(browser_pool.__aexit__(None, None, None))
    await exit_started.wait()

    with pytest.raises(RuntimeError, match='already active'):
        await browser_pool.__aenter__()

    release_exit.set()
    await exit_task


async def test_failed_browser_close_exits_plugins() -> None:
    """A pool whose browser fails to close still closes its other browsers and exits its plugins."""
    plugin = AsyncMock()
    failing_browser = AsyncMock(idle_time=timedelta(0))
    failing_browser.close.side_effect = RuntimeError('close failed')
    other_browser = AsyncMock(idle_time=timedelta(0))
    browser_pool = BrowserPool(plugins=[plugin])

    with pytest.raises(RuntimeError, match='close failed'):
        async with browser_pool:
            browser_pool._active_browsers.extend([failing_browser, other_browser])

    assert not browser_pool.active
    other_browser.close.assert_awaited_once()
    plugin.__aexit__.assert_awaited_once()


async def test_plugin_exit_error_keeps_cancellation(caplog: pytest.LogCaptureFixture) -> None:
    """A plugin that fails to exit is logged and doesn't replace a cancellation of the pool's exit."""
    failing_plugin = AsyncMock()
    failing_plugin.__aexit__.side_effect = RuntimeError('exit failed')
    close_started = asyncio.Event()

    async def close_until_cancelled(**_: object) -> None:
        close_started.set()
        await asyncio.Event().wait()

    browser = AsyncMock(idle_time=timedelta(0))
    browser.close.side_effect = close_until_cancelled
    browser_pool = BrowserPool(plugins=[failing_plugin])
    await browser_pool.__aenter__()
    browser_pool._active_browsers.append(browser)

    exit_task = asyncio.create_task(browser_pool.__aexit__(None, None, None))
    await close_started.wait()
    exit_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await exit_task

    failing_plugin.__aexit__.assert_awaited_once()
    assert 'exit failed' in caplog.text


async def test_cancelled_plugin_exit_logs_close_error(caplog: pytest.LogCaptureFixture) -> None:
    """A browser that fails to close is logged when the pool's exit is cancelled while exiting its plugins."""
    exit_started = asyncio.Event()

    async def exit_until_cancelled(*_: object) -> None:
        exit_started.set()
        await asyncio.Event().wait()

    plugin = AsyncMock()
    plugin.__aexit__.side_effect = exit_until_cancelled
    browser = AsyncMock(idle_time=timedelta(0))
    browser.close.side_effect = RuntimeError('close failed')
    browser_pool = BrowserPool(plugins=[plugin])
    await browser_pool.__aenter__()
    browser_pool._active_browsers.append(browser)

    exit_task = asyncio.create_task(browser_pool.__aexit__(None, None, None))
    await exit_started.wait()
    exit_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await exit_task

    assert not browser_pool.active
    assert 'close failed' in caplog.text


async def test_cancelled_browser_close_logs_sibling_error(caplog: pytest.LogCaptureFixture) -> None:
    """A browser that fails to close is logged when the pool's exit is cancelled while another browser still closes."""
    close_started = asyncio.Event()

    async def close_until_cancelled(**_: object) -> None:
        close_started.set()
        await asyncio.Event().wait()

    plugin = AsyncMock()
    failing_browser = AsyncMock(idle_time=timedelta(0))
    failing_browser.close.side_effect = RuntimeError('close failed')
    hanging_browser = AsyncMock(idle_time=timedelta(0))
    hanging_browser.close.side_effect = close_until_cancelled
    browser_pool = BrowserPool(plugins=[plugin])
    await browser_pool.__aenter__()
    browser_pool._active_browsers.extend([failing_browser, hanging_browser])

    exit_task = asyncio.create_task(browser_pool.__aexit__(None, None, None))
    await close_started.wait()
    exit_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await exit_task

    assert not browser_pool.active
    plugin.__aexit__.assert_awaited_once()
    assert 'close failed' in caplog.text


async def test_cancelled_task_stop_stops_the_other(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cancelled first task stop still lets the pool stop the other task, close its browsers and exit its plugins."""
    plugin = AsyncMock()
    browser = AsyncMock(idle_time=timedelta(0))
    browser_pool = BrowserPool(plugins=[plugin])
    close_stop = AsyncMock(wraps=browser_pool._close_inactive_browsers_task.stop)
    await browser_pool.__aenter__()
    browser_pool._active_browsers.append(browser)
    monkeypatch.setattr(
        browser_pool._identify_inactive_browsers_task, 'stop', AsyncMock(side_effect=asyncio.CancelledError())
    )
    monkeypatch.setattr(browser_pool._close_inactive_browsers_task, 'stop', close_stop)

    with pytest.raises(asyncio.CancelledError):
        await browser_pool.__aexit__(None, None, None)

    close_stop.assert_awaited_once()
    browser.close.assert_awaited_once()
    plugin.__aexit__.assert_awaited_once()
    monkeypatch.undo()
    await browser_pool._identify_inactive_browsers_task.stop()


async def test_failed_plugin_exit_exits_others() -> None:
    """A pool whose plugin fails to exit still exits its other plugins and raises the failure."""
    failing_plugin = AsyncMock()
    failing_plugin.__aexit__.side_effect = RuntimeError('exit failed')
    other_plugin = AsyncMock()
    browser_pool = BrowserPool(plugins=[failing_plugin, other_plugin])

    with pytest.raises(RuntimeError, match='exit failed'):
        async with browser_pool:
            pass

    assert not browser_pool.active
    other_plugin.__aexit__.assert_awaited_once()


async def test_extra_exit_failures_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    """A pool whose plugins fail to exit raises the first failure and logs the others."""
    first_plugin = AsyncMock()
    first_plugin.__aexit__.side_effect = RuntimeError('first failed')
    second_plugin = AsyncMock()
    second_plugin.__aexit__.side_effect = RuntimeError('second failed')
    browser_pool = BrowserPool(plugins=[first_plugin, second_plugin])

    with pytest.raises(RuntimeError, match='first failed'):
        async with browser_pool:
            pass

    assert 'Closing the browser pool failed.' in caplog.text
    assert 'second failed' in caplog.text
