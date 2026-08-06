from __future__ import annotations

import asyncio
import logging
import threading
from contextlib import suppress
from datetime import timedelta
from functools import update_wrapper
from typing import TYPE_CHECKING, Any
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee._utils.wait import wait_for_all_tasks_to_finish
from crawlee.events import Event, EventManager, EventSystemInfoData

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence


@pytest.fixture
async def event_manager() -> AsyncGenerator[EventManager, None]:
    # The teardown waits for the listeners left running by the test, so cap it - without a timeout, a listener that
    # never completes hangs the whole run instead of failing the test that left it behind.
    async with EventManager(close_timeout=timedelta(seconds=5)) as event_manager:
        yield event_manager


@pytest.fixture
def event_system_info_data() -> EventSystemInfoData:
    return MagicMock(spec=EventSystemInfoData)


@pytest.fixture
def async_listener() -> AsyncMock:
    async def async_listener(payload: Any) -> None:
        pass

    al = AsyncMock()
    update_wrapper(al, async_listener)
    return al


@pytest.fixture
def sync_listener() -> MagicMock:
    def sync_listener(payload: Any) -> None:
        pass

    sl = MagicMock()
    update_wrapper(sl, sync_listener)
    return sl


class ListenerWaitSpy:
    """Observes the waits the event manager makes, patched in over the real waiting helper on construction.

    It lets a test see which tasks a wait awaits and act at the exact moment a wait starts, so that ordering does
    not have to be approximated by sleeping.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.awaited_tasks: list[set[asyncio.Task[None]]] = []
        """Tasks awaited by each of the waits, in the order the waits started."""

        self.wait_started = asyncio.Event()
        """Set by a wait right before it starts awaiting, so once it is observed, the waiter is already blocked."""

        monkeypatch.setattr('crawlee.events._event_manager.wait_for_all_tasks_to_finish', self.wait)

    async def wait(
        self,
        tasks: Sequence[asyncio.Task[None]],
        *,
        logger: logging.Logger,
        timeout: timedelta | None = None,
    ) -> None:
        self.awaited_tasks.append(set(tasks))
        self.wait_started.set()
        await wait_for_all_tasks_to_finish(tasks, logger=logger, timeout=timeout)


async def test_emit_invokes_registered_sync_listener(
    sync_listener: MagicMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await event_manager.wait_for_all_listeners_to_complete()

    assert sync_listener.call_count == 1
    assert sync_listener.call_args[0] == (event_system_info_data,)


async def test_emit_invokes_both_sync_and_async_listeners(
    sync_listener: MagicMock,
    async_listener: AsyncMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await event_manager.wait_for_all_listeners_to_complete()

    assert async_listener.call_count == 1
    assert async_listener.call_args[0] == (event_system_info_data,)

    assert sync_listener.call_count == 1
    assert sync_listener.call_args[0] == (event_system_info_data,)


async def test_emit_starts_a_task_for_every_listener_invocation(
    sync_listener: MagicMock,
    async_listener: AsyncMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """Listener tasks are registered by `emit` itself, so a wait that follows it cannot miss any of them."""
    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    # The same listener registered twice is invoked twice.
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    assert len(event_manager._listener_tasks) == 3

    await event_manager.wait_for_all_listeners_to_complete()

    assert sync_listener.call_count == 1
    assert async_listener.call_count == 2


async def test_emit_event_with_no_listeners(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
    async_listener: AsyncMock,
) -> None:
    # Register a listener for a different event
    event_manager.on(event=Event.ABORTING, listener=async_listener)

    # Attempt to emit an event for which no listeners are registered, it should not fail
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
    await event_manager.wait_for_all_listeners_to_complete()

    # Ensure the listener for the other event was not called
    assert async_listener.call_count == 0


async def test_emit_invokes_parameterless_listener(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    sync_mock = MagicMock()

    def sync_listener() -> None:
        sync_mock()

    async_mock = MagicMock()

    async def async_listener() -> None:
        async_mock()

    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
    await event_manager.wait_for_all_listeners_to_complete()

    assert sync_mock.call_count == 1
    assert async_mock.call_count == 1


async def test_emit_logs_a_listener_that_cannot_be_called(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A listener that fits neither call shape must be reported like any other failing one, not escape its task."""

    async def two_parameter_listener(_first: Any, _second: Any) -> None:
        pass

    event_manager.on(event=Event.SYSTEM_INFO, listener=two_parameter_listener)  # ty: ignore[no-matching-overload]
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    with caplog.at_level(logging.ERROR):
        await event_manager.wait_for_all_listeners_to_complete()

    assert [record.message for record in caplog.records] == ['Exception in the event listener']


async def test_emit_invokes_async_callable_instance_listener(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """A class instance with an `async def __call__` is an async listener - it must be awaited, not run in a thread."""
    received: list[Any] = []

    class AsyncCallableListener:
        async def __call__(self, event_data: Any) -> None:
            received.append(event_data)

    event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncCallableListener())
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
    await event_manager.wait_for_all_listeners_to_complete()

    assert received == [event_system_info_data]


async def test_emit_invokes_sync_callable_instance_listener(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """A class instance with a plain `__call__` is a sync listener and is run in a separate thread."""
    received: list[Any] = []
    listener_thread_ids: list[int] = []

    class SyncCallableListener:
        def __call__(self, event_data: Any) -> None:
            received.append(event_data)
            listener_thread_ids.append(threading.get_ident())

    event_manager.on(event=Event.SYSTEM_INFO, listener=SyncCallableListener())
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
    await event_manager.wait_for_all_listeners_to_complete()

    assert received == [event_system_info_data]
    assert listener_thread_ids != [threading.get_ident()]


async def test_remove_nonexistent_listener_does_not_fail(
    async_listener: AsyncMock,
    event_manager: EventManager,
) -> None:
    # Attempt to remove a specific listener that was never added.
    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)
    # Attempt to remove all listeners.
    event_manager.off(event=Event.ABORTING)


async def test_removed_listener_not_invoked_on_emit(
    async_listener: AsyncMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await event_manager.wait_for_all_listeners_to_complete()
    assert async_listener.call_count == 0


async def test_off_with_a_falsy_listener_removes_only_that_listener(
    sync_listener: MagicMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """A listener that is falsy in a boolean context must not be mistaken for "no listener given"."""
    falsy_listener = MagicMock()
    falsy_listener.__bool__.return_value = False

    event_manager.on(event=Event.SYSTEM_INFO, listener=falsy_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    event_manager.off(event=Event.SYSTEM_INFO, listener=falsy_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await event_manager.wait_for_all_listeners_to_complete()

    assert falsy_listener.call_count == 0
    assert sync_listener.call_count == 1


async def test_off_leaves_no_entries_behind(
    async_listener: AsyncMock,
    event_manager: EventManager,
) -> None:
    """Removing a listener must not leave an entry holding a reference to it, nor register unknown events."""
    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.off(event=Event.ABORTING)

    assert event_manager._listeners_to_wrappers == {}

    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)

    # Reading with `get` - a plain lookup would create the very entry that is asserted to be gone.
    assert event_manager._listeners_to_wrappers.get(Event.SYSTEM_INFO) == {}


async def test_close_clears_listeners_and_tasks(async_listener: AsyncMock) -> None:
    async with EventManager() as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    assert async_listener.call_count == 0
    assert len(event_manager._listener_tasks) == 0
    assert len(event_manager._listeners_to_wrappers) == 0


async def test_close_after_emit_processes_event(
    async_listener: AsyncMock,
    event_system_info_data: EventSystemInfoData,
) -> None:
    async with EventManager() as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    # Event should be processed before the event manager is closed
    assert async_listener.call_count == 1
    assert async_listener.call_args[0] == (event_system_info_data,)

    assert len(event_manager._listener_tasks) == 0
    assert len(event_manager._listeners_to_wrappers) == 0


async def test_nested_context_tears_down_on_the_last_exit_only(
    async_listener: AsyncMock,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """A nested exit only flushes the running listeners, the outermost one tears the event manager down."""
    event_manager = EventManager()

    async with event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

        async with event_manager:
            event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

        assert async_listener.call_count == 1
        assert event_manager.active is True
        assert len(event_manager._listeners_to_wrappers) == 1

        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    assert async_listener.call_count == 2
    assert event_manager.active is False
    assert len(event_manager._listeners_to_wrappers) == 0


async def test_context_is_left_even_if_closing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A manager left active by a failed close would never emit `PersistState` again - re-entering it is a no-op."""
    event_manager = EventManager()
    await event_manager.__aenter__()

    async def raise_error() -> None:
        raise RuntimeError('Emitting failed.')

    monkeypatch.setattr(event_manager, '_emit_persist_state_event', raise_error)

    with pytest.raises(RuntimeError, match=r'Emitting failed\.'):
        await event_manager.__aexit__(None, None, None)

    assert event_manager.active is False


async def test_wait_for_all_listeners_cancelled_error(
    monkeypatch: pytest.MonkeyPatch,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """A cancelled wait must cancel the listeners it was awaiting and let the cancellation propagate."""
    never_set = asyncio.Event()
    listener_cancelled = asyncio.Event()

    async def never_ending_listener() -> None:
        try:
            await never_set.wait()
        except asyncio.CancelledError:
            listener_cancelled.set()
            raise

    async def cancel_the_wait(*_: Any, **__: Any) -> None:
        raise asyncio.CancelledError

    event_manager = EventManager()
    await event_manager.__aenter__()
    event_manager.on(event=Event.SYSTEM_INFO, listener=never_ending_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    # Only `asyncio.wait` is replaced, so the cancellation is still handled by the real waiting code - which is what
    # has to cancel the listener that never finishes on its own.
    monkeypatch.setattr('asyncio.wait', cancel_the_wait)

    # Capped, as a listener left uncancelled would be awaited forever - the close has no timeout of its own here.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(event_manager.__aexit__(None, None, None), timeout=5)

    assert listener_cancelled.is_set()


async def test_methods_raise_error_when_not_active(event_system_info_data: EventSystemInfoData) -> None:
    event_manager = EventManager()

    assert event_manager.active is False

    with pytest.raises(RuntimeError, match=r'EventManager is not active.'):
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    with pytest.raises(RuntimeError, match=r'EventManager is not active.'):
        await event_manager.wait_for_all_listeners_to_complete()

    with pytest.raises(RuntimeError, match=r'EventManager is not active.'):
        await event_manager.__aexit__(None, None, None)

    async with event_manager:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
        await event_manager.wait_for_all_listeners_to_complete()

        assert event_manager.active is True


async def test_wait_for_all_listeners_from_within_a_listener_does_not_deadlock(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """Waiting from within a listener must not self-await, yet must still await the other listeners."""
    parked = asyncio.Event()
    release_other_listener = asyncio.Event()
    other_listener_done = asyncio.Event()
    waiter_done = asyncio.Event()
    other_done_when_wait_returned: bool | None = None

    async def other_listener(_: Any) -> None:
        await release_other_listener.wait()
        other_listener_done.set()

    async def waiting_listener(_: Any) -> None:
        nonlocal other_done_when_wait_returned
        # Set right before the wait, which registers the waiter and captures the tasks without yielding in between.
        parked.set()
        await event_manager.wait_for_all_listeners_to_complete()
        other_done_when_wait_returned = other_listener_done.is_set()
        waiter_done.set()

    event_manager.on(event=Event.SYSTEM_INFO, listener=other_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=waiting_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    # The other listener finishes only once the waiter is already blocked, so the wait cannot pass it by.
    await asyncio.wait_for(parked.wait(), timeout=5)
    release_other_listener.set()

    await asyncio.wait_for(waiter_done.wait(), timeout=5)

    # No self-await deadlock, and the wait must have blocked until the co-registered listener finished.
    assert other_done_when_wait_returned is True


async def test_wait_from_within_multiple_listeners_does_not_deadlock(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """Several listeners each waiting for all listeners at once must not deadlock one another."""
    first_done = asyncio.Event()
    second_done = asyncio.Event()

    async def first_waiting_listener(_: Any) -> None:
        await event_manager.wait_for_all_listeners_to_complete()
        first_done.set()

    async def second_waiting_listener(_: Any) -> None:
        await event_manager.wait_for_all_listeners_to_complete()
        second_done.set()

    event_manager.on(event=Event.SYSTEM_INFO, listener=first_waiting_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=second_waiting_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await asyncio.wait_for(asyncio.gather(first_done.wait(), second_done.wait()), timeout=5)

    assert first_done.is_set()
    assert second_done.is_set()


async def test_wait_from_outside_awaits_a_listener_that_is_itself_waiting(
    monkeypatch: pytest.MonkeyPatch,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """A caller that is not a listener is outside the deadlock cycle, so it must await even waiting listeners."""
    spy = ListenerWaitSpy(monkeypatch)
    parked = asyncio.Event()
    release_other_listener = asyncio.Event()
    waiting_listener_done = asyncio.Event()
    waiter_task: asyncio.Task[None] | None = None

    async def other_listener(_: Any) -> None:
        await release_other_listener.wait()

    async def waiting_listener(_: Any) -> None:
        nonlocal waiter_task
        waiter_task = asyncio.current_task()
        # Set right before the wait, which registers the waiter and captures the tasks without yielding in between.
        parked.set()
        await event_manager.wait_for_all_listeners_to_complete()
        waiting_listener_done.set()

    event_manager.on(event=Event.SYSTEM_INFO, listener=other_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=waiting_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await asyncio.wait_for(parked.wait(), timeout=5)

    # The release has to come from outside this task, as the wait below blocks until the listeners are done. Scheduled
    # on the loop, it runs only once that wait has suspended - so the wait captures the tasks with both still running.
    asyncio.get_running_loop().call_soon(release_other_listener.set)
    await asyncio.wait_for(event_manager.wait_for_all_listeners_to_complete(), timeout=5)

    # The waiter's own wait went first, so the second one is the outer wait - and it had to include the parked waiter.
    assert waiter_task in spy.awaited_tasks[1]
    assert waiting_listener_done.is_set()


async def test_close_from_within_a_listener_does_not_deadlock_or_error(
    monkeypatch: pytest.MonkeyPatch,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """Closing the event manager from within a listener (as `Actor.exit()` does) must not deadlock or raise."""
    spy = ListenerWaitSpy(monkeypatch)
    event_manager = EventManager()
    await event_manager.__aenter__()

    # A listener task finalizing after the close cleared the task set must not report a stray exception.
    loop_errors: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    original_exception_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))

    closed = asyncio.Event()
    release_other_listener = asyncio.Event()
    other_listener_done = asyncio.Event()
    closing_task: asyncio.Task[None] | None = None

    async def other_listener(_: Any) -> None:
        await release_other_listener.wait()
        other_listener_done.set()

    async def closing_listener(_: Any) -> None:
        nonlocal closing_task
        closing_task = asyncio.current_task()
        await event_manager.__aexit__(None, None, None)
        closed.set()

    # A second listener makes close await a concurrently-running listener - the real `Actor.exit()` shape.
    event_manager.on(event=Event.SYSTEM_INFO, listener=other_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=closing_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    try:
        # The close is already blocked in its wait here, so releasing the other listener only now means the close
        # really does have to await a listener that is still running.
        await asyncio.wait_for(spy.wait_started.wait(), timeout=5)
        release_other_listener.set()

        await asyncio.wait_for(closed.wait(), timeout=5)
        # Let the closing listener's own task finalize before asserting - its wrapper returns right after `closed`.
        assert closing_task is not None
        await asyncio.wait_for(closing_task, timeout=5)
    finally:
        loop.set_exception_handler(original_exception_handler)
        # Cap the cleanup so a regressed deadlock surfaces the real failure instead of hanging.
        if event_manager.active:
            with suppress(Exception):
                await asyncio.wait_for(event_manager.__aexit__(None, None, None), timeout=5)

    assert loop_errors == []
    assert other_listener_done.is_set()
    assert event_manager.active is False
    assert len(event_manager._listener_tasks) == 0


async def test_event_manager_in_context_persistence() -> None:
    """Test that entering the `EventManager` context emits persist state event at least once."""
    event_manager = EventManager()

    with mock.patch.object(event_manager, '_emit_persist_state_event', AsyncMock()) as mocked_emit_persist_state_event:
        async with event_manager:
            pass

    assert mocked_emit_persist_state_event.call_count >= 1
