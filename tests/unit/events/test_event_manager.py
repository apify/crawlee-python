from __future__ import annotations

import asyncio
import gc
import logging
import multiprocessing
import sys
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress
from datetime import timedelta
from functools import update_wrapper
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crawlee import service_locator
from crawlee._utils.wait import wait_for_all_tasks_to_finish
from crawlee.events import Event, EventManager, EventPersistStateData, EventSystemInfoData, LocalEventManager

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator, Sequence


@pytest.fixture
def collect_abandoned_tasks() -> Iterator[None]:
    # The tasks that a test leaves pending in an event loop closed without a shutdown log an error once garbage
    # collected, so collect them in the teardown of that test instead of in whichever test happens to run later.
    yield
    gc.collect()


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


def _registered_listeners(event_manager: EventManager, event: Event) -> list[Any]:
    return list(event_manager._listeners_to_wrappers.get(event, {}))


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


async def test_exit_keeps_listeners(async_listener: AsyncMock) -> None:
    """The exit stops the manager, but its listeners stay registered."""
    event_manager = EventManager()

    async with event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    assert event_manager.active is False
    assert async_listener.call_count == 0
    assert len(event_manager._listener_tasks) == 0
    assert _registered_listeners(event_manager, Event.SYSTEM_INFO) == [async_listener]


async def test_close_after_emit_processes_event(
    async_listener: AsyncMock,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """The exit waits for the listeners that are still running."""

    # Still running once the exit has stopped the recurring task, so only the wait for the listeners lets it finish.
    async def slow_listener(event_data: EventSystemInfoData) -> None:
        await asyncio.sleep(0.1)
        await async_listener(event_data)

    async with EventManager() as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=slow_listener)
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    # Event should be processed before the event manager is closed
    assert async_listener.call_count == 1
    assert async_listener.call_args[0] == (event_system_info_data,)

    assert len(event_manager._listener_tasks) == 0


async def test_enter_twice_raises() -> None:
    """A manager can be entered only once at a time."""
    async with EventManager() as event_manager:
        with pytest.raises(RuntimeError, match=r'EventManager is already entered'):
            await event_manager.__aenter__()


async def test_exit_emits_final_persist_state(async_listener: AsyncMock) -> None:
    """The exit emits a final `PersistState` event and stops the manager, also when it was used inside the block."""
    async with EventManager() as event_manager:
        event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    assert async_listener.call_count == 1
    assert event_manager.active is False


async def test_enter_after_lazy_start_takes_over(async_listener: AsyncMock) -> None:
    """Entering a manager that started by itself takes over its lifecycle, so the exit stops it."""
    event_manager = EventManager()
    event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    async with event_manager:
        assert event_manager.active is True

    assert event_manager.active is False
    async_listener.assert_called_once()


async def test_emit_after_exit_does_not_restart(async_listener: AsyncMock) -> None:
    """An event emitted after the owner's exit reaches the listeners without starting the manager again."""
    event_manager = EventManager()

    async with event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    await asyncio.gather(*event_manager.emit(event=Event.SYSTEM_INFO, event_data=None))

    async_listener.assert_called_once()
    assert event_manager.active is False


def test_on_during_exit_restarts(async_listener: AsyncMock) -> None:
    """A listener registered while the owner's exit waits for the listeners starts the manager again."""
    event_manager = EventManager()

    async def register_listener() -> None:
        event_manager.off(event=Event.PERSIST_STATE, listener=register_listener)
        event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    async def use() -> bool:
        async with event_manager:
            event_manager.on(event=Event.PERSIST_STATE, listener=register_listener)

        return event_manager.active

    assert asyncio.run(use()) is True
    # Registered after the final event of the exit, so the call comes from the shutdown of the event loop.
    async_listener.assert_called_once()


async def test_context_is_left_even_if_closing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed close still stops the manager, as one left active could never be entered again."""
    event_manager = EventManager()
    await event_manager.__aenter__()

    monkeypatch.setattr(
        event_manager, '_emit_persist_state_event', MagicMock(side_effect=RuntimeError('Emitting failed.'))
    )

    with pytest.raises(RuntimeError, match=r'Emitting failed\.'):
        await event_manager.__aexit__(None, None, None)

    assert event_manager.active is False


async def test_final_persist_state_follows_running_listeners(monkeypatch: pytest.MonkeyPatch) -> None:
    """The exit emits its final `PersistState` event only once the listeners still running have finished."""
    spy = ListenerWaitSpy(monkeypatch)
    earlier_event_data = EventPersistStateData(is_migrating=False)
    release_earlier = asyncio.Event()
    saved = MagicMock()

    async def save(event_data: EventPersistStateData) -> None:
        if event_data is earlier_event_data:
            await release_earlier.wait()
        saved(event_data)

    async def release_once_waiting() -> None:
        await spy.wait_started.wait()
        release_earlier.set()

    async with EventManager() as event_manager:
        event_manager.on(event=Event.PERSIST_STATE, listener=save)
        event_manager.emit(event=Event.PERSIST_STATE, event_data=earlier_event_data)
        releaser = asyncio.create_task(release_once_waiting())

    await releaser

    assert saved.call_count == 2
    assert saved.call_args_list[0].args[0] is earlier_event_data


async def test_final_persist_state_skips_other_listeners() -> None:
    """The final `PersistState` event doesn't wait for the listeners of other events that are still running."""
    release_other_listener = asyncio.Event()
    persisted = asyncio.Event()

    async def other_listener() -> None:
        await release_other_listener.wait()

    async def save() -> None:
        persisted.set()

    event_manager = EventManager()
    await event_manager.__aenter__()
    event_manager.on(event=Event.SYSTEM_INFO, listener=other_listener)
    event_manager.on(event=Event.PERSIST_STATE, listener=save)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=None)
    exit_task = asyncio.create_task(event_manager.__aexit__(None, None, None))

    try:
        await asyncio.wait_for(persisted.wait(), timeout=5)
    finally:
        release_other_listener.set()
        await exit_task


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


async def test_strict_manager_needs_enter(
    monkeypatch: pytest.MonkeyPatch,
    async_listener: AsyncMock,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """A manager that doesn't start by itself rejects `on` until it's entered, but not `emit` or `off`."""
    event_manager = EventManager()
    monkeypatch.setattr(event_manager, 'supports_lazy_start', False)

    with pytest.raises(RuntimeError, match=r"doesn't start by itself"):
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    assert event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data) == []

    with pytest.raises(RuntimeError, match=r'EventManager is not active'):
        await event_manager.__aexit__(None, None, None)

    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)

    async with event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
        await event_manager.wait_for_all_listeners_to_complete()

    assert async_listener.call_count == 1
    assert event_manager.active is False


async def test_strict_manager_on_from_thread_registers(
    monkeypatch: pytest.MonkeyPatch,
    async_listener: AsyncMock,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """An entered manager that doesn't start by itself registers a listener from a thread without an event loop."""
    event_manager = EventManager()
    monkeypatch.setattr(event_manager, 'supports_lazy_start', False)

    async with event_manager:
        await asyncio.to_thread(lambda: event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener))
        await asyncio.gather(*event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data))

    assert async_listener.call_count == 1


async def test_strict_manager_use_from_other_loop_raises(
    monkeypatch: pytest.MonkeyPatch,
    async_listener: AsyncMock,
) -> None:
    """An entered manager that doesn't start by itself can't be used from another running event loop."""
    event_manager = EventManager()
    monkeypatch.setattr(event_manager, 'supports_lazy_start', False)

    async def use() -> None:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    async with event_manager:
        with pytest.raises(RuntimeError, match=r'belongs to another event loop that is still running'):
            await asyncio.to_thread(asyncio.run, use())

    assert event_manager._listeners_to_wrappers == {}


def test_exit_from_other_loop_raises() -> None:
    """Exiting the manager from another event loop than the one it runs in raises that it isn't active there."""
    event_manager = EventManager()

    with asyncio.Runner() as runner:
        runner.run(event_manager.__aenter__())

        with pytest.raises(RuntimeError, match=r'EventManager is not active'):
            asyncio.run(event_manager.__aexit__(None, None, None))


@pytest.mark.usefixtures('collect_abandoned_tasks')
def test_closed_loop_leaves_inactive(async_listener: AsyncMock) -> None:
    """A manager whose event loop closed without shutting it down isn't active."""
    event_manager = EventManager()

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(register())
        assert event_manager.active is True
    finally:
        loop.close()

    assert event_manager.active is False


@pytest.mark.usefixtures('collect_abandoned_tasks')
def test_strict_gone_loop_needs_entry(monkeypatch: pytest.MonkeyPatch, async_listener: AsyncMock) -> None:
    """A strict manager left active by an event loop closed without exiting it has to be entered again."""
    event_manager = EventManager()
    monkeypatch.setattr(event_manager, 'supports_lazy_start', False)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(event_manager.__aenter__())
    finally:
        loop.close()

    async def use() -> None:
        with pytest.raises(RuntimeError, match=r"doesn't start by itself"):
            event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

        async with event_manager:
            event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    asyncio.run(use())


def test_strict_manager_of_stopped_loop_can_be_entered(
    monkeypatch: pytest.MonkeyPatch,
    async_listener: AsyncMock,
) -> None:
    """A strict manager left running in a stopped event loop isn't active in another one, so an owner enters it."""
    event_manager = EventManager()
    monkeypatch.setattr(event_manager, 'supports_lazy_start', False)

    async def use_if_inactive() -> None:
        if not event_manager.active:
            async with event_manager:
                event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    with asyncio.Runner() as stopped_runner:
        stopped_runner.run(event_manager.__aenter__())
        asyncio.run(use_if_inactive())

    assert async_listener.call_count == 1


@pytest.mark.parametrize(
    'manager_class',
    [
        pytest.param(EventManager, id='event-manager'),
        pytest.param(LocalEventManager, id='local-event-manager'),
    ],
)
def test_moving_on_cancels_stopped_loop_tasks(manager_class: type[EventManager]) -> None:
    """Moving to another event loop cancels the tasks the manager left in a stopped one, so they never run again."""
    event_manager = manager_class()

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=AsyncMock())

    with asyncio.Runner() as stopped_runner:
        stopped_runner.run(register())
        # Kept here, as a guard that nothing cancels would be garbage collected and drop out of the tasks below.
        guard = event_manager._shutdown_task
        asyncio.run(register())
        left_tasks = asyncio.all_tasks(stopped_runner.get_loop())

        assert guard is not None
        assert guard.cancelling()
        assert all(task.cancelling() for task in left_tasks)


def test_resumed_loop_spares_moved_manager(async_listener: AsyncMock) -> None:
    """The shutdown of an event loop that the manager moved away from doesn't close it in its new loop."""
    event_manager = EventManager()

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    with asyncio.Runner() as new_runner:
        with asyncio.Runner() as old_runner:
            old_runner.run(register())
            new_runner.run(register())

        assert async_listener.call_count == 0
        assert event_manager.active is True

    # The final event comes from the shutdown of the new event loop.
    assert async_listener.call_count == 1


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

    # Let the done callbacks of the finished listener tasks run.
    await asyncio.sleep(0)

    assert loop_errors == []
    assert other_listener_done.is_set()
    assert event_manager.active is False
    assert len(event_manager._listener_tasks) == 0


async def test_on_starts_manager(async_listener: AsyncMock) -> None:
    """Registering a listener within an event loop starts the manager."""
    event_manager = EventManager()

    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    assert event_manager.active is True


def test_on_outside_loop_only_registers(async_listener: AsyncMock) -> None:
    """A listener registered outside an event loop is kept, and emitting to it within one doesn't start the manager."""
    event_manager = EventManager()
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    assert event_manager.active is False

    async def emit() -> None:
        listener_tasks = event_manager.emit(event=Event.SYSTEM_INFO, event_data=None)
        assert event_manager.active is False
        await asyncio.gather(*listener_tasks)

    asyncio.run(emit())

    assert async_listener.call_count == 1


async def test_emit_returns_listener_tasks(
    event_manager: EventManager,
    async_listener: AsyncMock,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """The tasks `emit` returns run the listeners of that emission."""
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    listener_tasks = event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
    await asyncio.gather(*listener_tasks)

    assert len(listener_tasks) == 1
    assert async_listener.call_count == 1


def test_loop_end_emits_persist_state(async_listener: AsyncMock) -> None:
    """When its event loop shuts down, the manager emits a last `PersistState` event and stops."""
    event_manager = EventManager()

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    asyncio.run(register())

    assert async_listener.call_count == 1
    assert async_listener.call_args[0][0].is_migrating is False
    assert event_manager.active is False


def test_loop_end_drops_listeners(async_listener: AsyncMock) -> None:
    """The shutdown of the event loop leaves no listeners behind."""
    event_manager = EventManager()

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    asyncio.run(register())

    assert event_manager._listeners_to_wrappers == {}


def test_loop_end_drops_listeners_after_exit(async_listener: AsyncMock) -> None:
    """The listeners are dropped with the event loop also when the owner stopped the manager before."""
    event_manager = EventManager()

    async def use_and_exit() -> None:
        async with event_manager:
            event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

        assert _registered_listeners(event_manager, Event.PERSIST_STATE) == [async_listener]

    asyncio.run(use_and_exit())

    assert event_manager._listeners_to_wrappers == {}


def test_exit_then_loop_end_persists_once(async_listener: AsyncMock) -> None:
    """After an owner stopped the manager, the shutdown of the event loop emits no second final `PersistState` event."""
    event_manager = EventManager()

    async def use_and_exit() -> None:
        async with event_manager:
            event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    asyncio.run(use_and_exit())

    assert async_listener.call_count == 1


def test_restart_after_exit_closes_once(async_listener: AsyncMock) -> None:
    """A manager restarted after an exit in the same event loop emits one final `PersistState` event at its end."""
    event_manager = EventManager()

    async def exit_and_restart() -> None:
        async with event_manager:
            event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())

    asyncio.run(exit_and_restart())

    # One final event for the exit, and one for the shutdown of the event loop.
    assert async_listener.call_count == 2


@pytest.mark.usefixtures('collect_abandoned_tasks')
def test_abandoned_loop_leaves_nothing(async_listener: AsyncMock) -> None:
    """What an event loop closed without a shutdown left in the manager is dropped on its next use."""
    event_manager = EventManager()

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(register())
    finally:
        loop.close()

    async def use() -> bool:
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())
        return event_manager.active

    assert asyncio.run(use()) is True
    # The listener of the gone event loop is dropped, so not even the final `PersistState` event reaches it.
    assert async_listener.call_count == 0


def test_abandoned_owner_loop_can_be_entered() -> None:
    """A manager entered in an event loop closed without a shutdown can be entered after it starts in the next one."""
    event_manager = EventManager()

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(event_manager.__aenter__())
    finally:
        loop.close()

    async def use() -> bool:
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())

        async with event_manager:
            return event_manager.active

    assert asyncio.run(use()) is True


def test_abandoned_guard_spares_next_loop() -> None:
    """The pending guard of an event loop closed without a shutdown doesn't stop the manager in the next loop."""
    event_manager = EventManager()

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=AsyncMock())

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(register())
    finally:
        loop.close()

    async def use() -> bool:
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())
        # Finalizes the guard of the closed event loop while this one runs.
        gc.collect()
        await asyncio.sleep(0)
        recurring_task = event_manager._emit_persist_state_event_rec_task.task
        return recurring_task is not None and not recurring_task.done()

    assert asyncio.run(use()) is True


async def test_use_from_other_loop_raises(async_listener: AsyncMock) -> None:
    """A manager running in one event loop can't be used from another one that runs at the same time."""
    event_manager = EventManager()
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    async def use() -> None:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=None)

    with pytest.raises(RuntimeError, match=r'belongs to another event loop that is still running'):
        await asyncio.to_thread(asyncio.run, use())


def test_loop_end_wait_has_time_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """A listener that never finishes is cancelled once the shutdown time limit passes."""
    monkeypatch.setattr(EventManager, '_SHUTDOWN_TIMEOUT', timedelta(milliseconds=100))
    on_cancel = MagicMock()
    event_manager = EventManager()

    async def never_ending_listener() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            on_cancel()
            raise

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=never_ending_listener)

    asyncio.run(register())

    on_cancel.assert_called_once()


def test_loop_end_wait_uses_close_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """The shutdown of the event loop waits for the listeners for `close_timeout` when it's set."""
    wait_mock = AsyncMock(wraps=wait_for_all_tasks_to_finish)
    monkeypatch.setattr('crawlee.events._event_manager.wait_for_all_tasks_to_finish', wait_mock)
    close_timeout = timedelta(seconds=5)
    event_manager = EventManager(close_timeout=close_timeout)

    async def register() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=AsyncMock())

    asyncio.run(register())

    # One wait for the running listeners, and one for those of the final event.
    assert wait_mock.await_count == 2
    assert all(call.kwargs['timeout'] == close_timeout for call in wait_mock.await_args_list)


async def test_first_persist_state_delayed(async_listener: AsyncMock) -> None:
    """A started manager emits its first periodic `PersistState` event only after the interval."""
    event_manager = EventManager(persist_state_interval=timedelta(hours=1))
    event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    for _ in range(10):
        await asyncio.sleep(0)

    assert async_listener.call_count == 0


async def _wait_until_closed(event_manager: EventManager) -> None:
    for _ in range(1000):
        if event_manager._loop is None:
            return
        await asyncio.sleep(0)


def test_use_at_loop_end_no_restart() -> None:
    """A task that listens and emits in its cleanup after the shutdown closed the manager doesn't start it again."""
    event_manager = EventManager()
    background_tasks: list[asyncio.Task[None]] = []
    record_emission = MagicMock()

    async def use_in_cleanup() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            # Like the teardown of a crawler, which emits its final event once the rest of the cleanup is done.
            await _wait_until_closed(event_manager)
            event_manager.on(event=Event.PERSIST_STATE, listener=AsyncMock())
            event_data = EventPersistStateData(is_migrating=False)
            record_emission(event_manager.emit(event=Event.PERSIST_STATE, event_data=event_data))

    async def main() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=AsyncMock())
        background_tasks.append(asyncio.create_task(use_in_cleanup()))
        await asyncio.sleep(0)

    asyncio.run(main())

    record_emission.assert_called_once_with([])
    assert event_manager.active is False
    assert event_manager._loop is None
    assert event_manager._listeners_to_wrappers == {}


async def test_close_during_owner_exit_stops_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """A close by the event loop during an owner's exit runs its own stop, as it may cancel the owner's one."""
    spy = ListenerWaitSpy(monkeypatch)
    release_save = asyncio.Event()

    async def slow_save() -> None:
        await release_save.wait()

    event_manager = EventManager()
    await event_manager.__aenter__()
    event_manager.on(event=Event.PERSIST_STATE, listener=slow_save)
    shutdown_task = event_manager._shutdown_task
    assert shutdown_task is not None

    # The owner emits its final event and waits for it, then the guard starts closing and waits for that event too.
    exit_task = asyncio.create_task(event_manager.__aexit__(None, None, None))
    await asyncio.wait_for(spy.wait_started.wait(), timeout=5)
    shutdown_task.cancel()

    for _ in range(100):
        # Two waits of the owner and the first one of the guard.
        if len(spy.awaited_tasks) == 3:
            break
        await asyncio.sleep(0)

    assert len(spy.awaited_tasks) == 3

    # The owner finishes first, so the guard emits its final event to a manager that is no longer active.
    release_save.set()
    await exit_task
    await asyncio.gather(shutdown_task, return_exceptions=True)

    recurring_task = event_manager._emit_persist_state_event_rec_task.task
    assert recurring_task is not None
    assert recurring_task.done()


async def test_on_after_close_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    """`on()` in an event loop that the manager closed with registers nothing and logs a warning, once."""
    event_manager = EventManager()
    event_manager.on(event=Event.PERSIST_STATE, listener=AsyncMock())
    shutdown_task = event_manager._shutdown_task
    assert shutdown_task is not None
    # The guard starts waiting long before an app cancels the tasks of the event loop.
    await asyncio.sleep(0)

    # An app cancelling all tasks of an event loop that keeps running closes the manager, as a shutdown would.
    shutdown_task.cancel()
    await asyncio.gather(shutdown_task, return_exceptions=True)

    with caplog.at_level(logging.WARNING, logger='crawlee.events._event_manager'):
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())

    assert event_manager._listeners_to_wrappers == {}
    expected_message = (
        'The event manager closed when the tasks of its event loop were cancelled, so it does not register the '
        'listener for the systemInfo event in that loop.'
    )
    assert [record.message for record in caplog.records] == [expected_message]


def test_late_owner_exit_is_noop() -> None:
    """An owner in a task cancelled by the shutdown exits without an error after the shutdown closed the manager."""
    event_manager = EventManager()
    owner_tasks: list[asyncio.Task[None]] = []

    async def own_the_manager() -> None:
        async with event_manager:
            try:
                await asyncio.Event().wait()
            finally:
                await _wait_until_closed(event_manager)

    async def main() -> None:
        owner_tasks.append(asyncio.create_task(own_the_manager()))
        await asyncio.sleep(0)

    asyncio.run(main())

    assert owner_tasks[0].cancelled()


def test_enter_at_loop_end_is_noop() -> None:
    """Entering the manager after the shutdown closed it doesn't start it again."""
    event_manager = EventManager()
    background_tasks: list[asyncio.Task[None]] = []
    record_active = MagicMock()

    async def enter_in_cleanup() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            await _wait_until_closed(event_manager)
            async with event_manager:
                record_active(event_manager.active)

    async def main() -> None:
        event_manager.on(event=Event.PERSIST_STATE, listener=AsyncMock())
        background_tasks.append(asyncio.create_task(enter_in_cleanup()))
        await asyncio.sleep(0)

    asyncio.run(main())

    record_active.assert_called_once()
    assert record_active.call_args.args[0] is False


def test_on_after_closed_loop_registers(async_listener: AsyncMock) -> None:
    """After the event loop the manager closed with is gone, `on` outside a loop registers the listener."""
    event_manager = EventManager()

    async def use() -> None:
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())

    asyncio.run(use())
    event_manager.on(event=Event.PERSIST_STATE, listener=async_listener)

    assert _registered_listeners(event_manager, Event.PERSIST_STATE) == [async_listener]


@pytest.mark.usefixtures('collect_abandoned_tasks')
def test_on_after_abandoned_loop_registers(async_listener: AsyncMock) -> None:
    """A listener registered outside an event loop after one closed without a shutdown reaches the next event loop."""
    event_manager = EventManager()

    async def start() -> None:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=None)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(start())
    finally:
        loop.close()

    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    async def emit() -> None:
        await asyncio.gather(*event_manager.emit(event=Event.SYSTEM_INFO, event_data=None))

    asyncio.run(emit())

    assert async_listener.call_count == 1


async def test_periodic_event_uses_emit(event_manager: EventManager) -> None:
    """The periodic `PersistState` event goes through `emit`, so a subclass that overrides it sees the event."""
    with patch.object(event_manager, 'emit', wraps=event_manager.emit) as emit:
        event_manager._emit_persist_state_event()

    emit.assert_called_once()
    assert emit.call_args.kwargs['event'] == Event.PERSIST_STATE


def _use_manager_in_child() -> int:
    async def use() -> int:
        event_manager = service_locator.get_event_manager()
        inherited_listeners = len(event_manager._listeners_to_wrappers)
        event_manager.on(event=Event.PERSIST_STATE, listener=lambda: None)
        return inherited_listeners

    return asyncio.run(use())


@pytest.mark.skipif(sys.platform != 'linux', reason='Forking a multithreaded process is reliable only on Linux.')
@pytest.mark.filterwarnings('ignore:.*use of fork\\(\\) may lead to deadlocks:DeprecationWarning')
async def test_forked_child_starts_afresh() -> None:
    """A child forked while the manager runs uses it without the event loop and the listeners of its parent."""
    service_locator.get_event_manager().on(event=Event.PERSIST_STATE, listener=AsyncMock())

    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context('fork')) as pool:
        inherited_listeners = await asyncio.get_running_loop().run_in_executor(pool, _use_manager_in_child)

    assert inherited_listeners == 0


@pytest.mark.skipif(sys.platform != 'linux', reason='Forking a multithreaded process is reliable only on Linux.')
@pytest.mark.filterwarnings('ignore:.*use of fork\\(\\) may lead to deadlocks:DeprecationWarning')
def test_forked_child_keeps_listeners_without_loop() -> None:
    """A child forked before the manager started keeps the listeners registered outside an event loop."""
    service_locator.get_event_manager().on(event=Event.PERSIST_STATE, listener=AsyncMock())

    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context('fork')) as pool:
        inherited_listeners = pool.submit(_use_manager_in_child).result()

    assert inherited_listeners == 1
