from __future__ import annotations

import asyncio
import inspect
import os
import weakref
from collections import defaultdict
from datetime import timedelta
from logging import WARNING, getLogger
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast, overload

from crawlee._utils.docs import docs_group
from crawlee._utils.log import LoggerOnce
from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.wait import wait_for_all_tasks_to_finish
from crawlee.events._types import (
    Event,
    EventAbortingData,
    EventCrawlerStatusData,
    EventExitData,
    EventListener,
    EventMigratingData,
    EventPersistStateData,
    EventSystemInfoData,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType
    from typing import NotRequired, Self

    from crawlee.events._types import EventData, WrappedListener

logger = getLogger(__name__)


class EventManagerOptions(TypedDict):
    """Arguments for the `EventManager` constructor.

    It is intended for typing forwarded `__init__` arguments in the subclasses.
    """

    persist_state_interval: NotRequired[timedelta]
    """Interval between emitted `PersistState` events to maintain state persistence."""

    close_timeout: NotRequired[timedelta | None]
    """Optional timeout for canceling pending event listeners if they exceed this duration. `None` waits without
    a limit, or 30 seconds at the shutdown of the event loop. Zero cancels them right away. A stop waits in two
    rounds, so it can take up to twice as long. A crawler waits for the listeners at the end of a run with the same
    limit.
    """


@docs_group('Event managers')
class EventManager:
    """Manage events and their listeners, enabling registration, emission, and execution control.

    Listeners can be registered for any of the events and are invoked whenever the event is emitted, each of them
    in its own task - the sync ones in a separate thread so that they cannot block the event loop. On top of that,
    it emits `PersistState` events at regular intervals and can wait for all the running listeners to complete.

    The manager starts by itself on the first `on` or `emit` call in a running event loop, and keeps running until
    that loop shuts down, when it emits a final `PersistState` event and drops the listeners. Whoever enters an
    inactive manager with `async with` owns it instead, and it stops on the exit. A manager that has to await something
    to start, such as connecting somewhere, sets `supports_lazy_start` to `False`, and calling `on` while it isn't
    active raises an error. Once its event loop starts shutting down, nothing starts the manager again in that loop,
    and `on` and `async with` do nothing there.
    """

    supports_lazy_start: ClassVar[bool] = True
    """Whether the manager starts by itself on the first `on` or `emit` call, instead of being entered first."""

    _SHUTDOWN_TIMEOUT = timedelta(seconds=30)
    """Time limit for each wait for the listeners when the event loop shuts down, unless `close_timeout` sets one."""

    _instances: ClassVar[weakref.WeakSet[EventManager]] = weakref.WeakSet()
    """Live managers, reset in a forked child process."""

    def __init__(
        self,
        *,
        persist_state_interval: timedelta = timedelta(minutes=1),
        close_timeout: timedelta | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_state_interval: Interval between emitted `PersistState` events to maintain state persistence.
            close_timeout: Optional timeout for canceling pending event listeners if they exceed this duration.
                `None` waits without a limit, or 30 seconds at the shutdown of the event loop. Zero cancels them
                right away. A stop waits in two rounds, so it can take up to twice as long. A crawler waits for the
                listeners at the end of a run with the same limit.
        """
        self._persist_state_interval = persist_state_interval
        """Interval between the emitted `PersistState` events."""

        self._close_timeout = close_timeout
        """Timeout for the running listeners when the manager stops. Without it, a stop waits for them without a limit,
        except for the one at the shutdown of the event loop, which waits for `_SHUTDOWN_TIMEOUT`.
        """

        self._listener_tasks: dict[asyncio.Task[None], Event] = {}
        """Tasks of the running listener invocations with their events, kept both to keep them alive and to be able to
        wait for them.
        """

        self._waiting_listener_tasks: set[asyncio.Task[None]] = set()
        """Listener tasks currently blocked in a wait for the listeners.

        They are excluded from the waits made by the listeners themselves, as a waiter must never await itself.
        """

        self._listeners_to_wrappers: dict[Event, dict[EventListener[Any], list[WrappedListener]]] = defaultdict(
            lambda: defaultdict(list),
        )
        """Registered listeners and their wrappers, mapped as `event -> listener -> [wrapper_1, wrapper_2, ...]`."""

        self._emit_persist_state_event_rec_task = RecurringTask(
            func=self._emit_persist_state_event,
            delay=self._persist_state_interval,
            run_immediately=False,
        )
        """Recurring task emitting the `PersistState` events."""

        self._loop: asyncio.AbstractEventLoop | None = None
        """The event loop the manager started in, kept until that loop shuts down or the manager leaves it, even if the
        manager stops earlier.
        """

        self._shutdown_task: asyncio.Task[None] | None = None
        """Task that closes the manager once cancelled, which `asyncio.run` does when the event loop shuts down. Kept
        here, as the event loop holds only weak references to its tasks.
        """

        self._active = False
        """Whether the manager runs in `_loop`. An event loop that stops or closes without a shutdown leaves it set."""

        self._closed_loop: asyncio.AbstractEventLoop | None = None
        """The event loop whose shutdown closes the manager, set once that shutdown begins, so that nothing starts the
        manager again in that loop.
        """

        self._logger_once = LoggerOnce(logger)
        """Logs the warnings that would otherwise repeat, once per manager."""

        EventManager._instances.add(self)

    @property
    def active(self) -> bool:
        """Whether the manager is running, started by itself or entered with `async with`.

        Within an event loop, only a manager that runs in that loop is active.
        """
        if not self._active or self._loop is None:
            return False

        loop = self._get_running_loop()
        if loop is not None:
            return self._loop is loop

        # An event loop closed without a shutdown leaves the flag set.
        return not self._loop.is_closed()

    @property
    def close_timeout(self) -> timedelta | None:
        """Timeout for the running listeners when the manager stops.

        `None` waits without a limit, except at the shutdown of the event loop, where the manager waits for 30 seconds.
        """
        return self._close_timeout

    async def __aenter__(self) -> Self:
        """Start the manager and take over its lifecycle.

        Raises:
            RuntimeError: If the manager is already active, including when it started by itself, or if it belongs to
                another event loop that is still running.
        """
        loop = asyncio.get_running_loop()

        if loop is self._closed_loop:
            return self

        if self._active and self._loop is loop:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._leave_other_loop(loop)
        self._start(loop)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop the manager, emitting a final `PersistState` event and waiting for all the running listeners.

        Raises:
            RuntimeError: If the manager isn't active in the running event loop.
        """
        loop = asyncio.get_running_loop()

        # The shutdown of the event loop may close the manager before an owner running in a cancelled task exits.
        if loop is self._closed_loop:
            return

        if not self._active or self._loop is not loop:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        await self._stop(timeout=self._close_timeout)

    @overload
    def on(self, *, event: Literal[Event.PERSIST_STATE], listener: EventListener[EventPersistStateData]) -> None: ...
    @overload
    def on(self, *, event: Literal[Event.SYSTEM_INFO], listener: EventListener[EventSystemInfoData]) -> None: ...
    @overload
    def on(self, *, event: Literal[Event.MIGRATING], listener: EventListener[EventMigratingData]) -> None: ...
    @overload
    def on(self, *, event: Literal[Event.ABORTING], listener: EventListener[EventAbortingData]) -> None: ...
    @overload
    def on(self, *, event: Literal[Event.EXIT], listener: EventListener[EventExitData]) -> None: ...
    @overload
    def on(self, *, event: Literal[Event.CRAWLER_STATUS], listener: EventListener[EventCrawlerStatusData]) -> None: ...
    @overload
    def on(self, *, event: Event, listener: EventListener[None]) -> None: ...

    def on(self, *, event: Event, listener: EventListener[Any]) -> None:
        """Register an event listener for a specific event.

        Within a running event loop, it also starts the manager if it starts by itself. The listener stays registered
        until `off` removes it, or until the event loop the manager runs in shuts down.

        Args:
            event: The event to listen to.
            listener: The function (sync or async) which is to be called when the event is emitted.

        Raises:
            RuntimeError: If the manager doesn't start by itself and nobody has entered it, or if it belongs to another
                event loop that is still running.
        """
        # Wrapped first, so that an invalid listener raises before it starts the manager.
        listener_wrapper = self._wrap_listener(event, listener)
        loop = self._get_running_loop()

        if loop is None:
            # Outside an event loop there is nothing to start, so the listener is only registered. What an event loop
            # closed without a shutdown left behind goes first, or the next event loop would drop the listener with it.
            if self._loop is not None and self._loop.is_closed():
                self._forget_loop()
        elif loop is self._closed_loop:
            # The manager closed with this event loop, at its shutdown or after an app cancelled all its tasks, and it
            # doesn't start again in that loop.
            self._logger_once.log(
                'The event manager closed when the tasks of its event loop were cancelled, so it does not register '
                'the listener for the %s event in that loop.',
                event.value,
                key='listener-after-close',
                level=WARNING,
            )
            return
        else:
            self._start_on_use(loop)

        if not self.supports_lazy_start and not self._active:
            raise RuntimeError(
                f"The {self.__class__.__name__} doesn't start by itself. Enter it with `async with` before using it."
            )

        self._listeners_to_wrappers[event][listener].append(listener_wrapper)

    def off(self, *, event: Event, listener: EventListener[Any] | None = None) -> None:
        """Remove a specific listener or all listeners for an event.

        Args:
            event: The event for which to remove listeners.
            listener: The listener which is supposed to be removed. If not passed, all listeners of this event
                are removed.
        """
        # Explicit `None` check - a listener may be an object that is falsy in a boolean context.
        if listener is None:
            self._listeners_to_wrappers.pop(event, None)
            return

        # Popping the whole entry - an empty list of wrappers would keep a reference to a listener that is no longer
        # registered, and looking the mapping up with `get` does not create entries for unknown events.
        self._listeners_to_wrappers.get(event, {}).pop(listener, None)

    @overload
    def emit(
        self, *, event: Literal[Event.PERSIST_STATE], event_data: EventPersistStateData
    ) -> list[asyncio.Task[None]]: ...
    @overload
    def emit(
        self, *, event: Literal[Event.SYSTEM_INFO], event_data: EventSystemInfoData
    ) -> list[asyncio.Task[None]]: ...
    @overload
    def emit(self, *, event: Literal[Event.MIGRATING], event_data: EventMigratingData) -> list[asyncio.Task[None]]: ...
    @overload
    def emit(self, *, event: Literal[Event.ABORTING], event_data: EventAbortingData) -> list[asyncio.Task[None]]: ...
    @overload
    def emit(self, *, event: Literal[Event.EXIT], event_data: EventExitData) -> list[asyncio.Task[None]]: ...
    @overload
    def emit(
        self, *, event: Literal[Event.CRAWLER_STATUS], event_data: EventCrawlerStatusData
    ) -> list[asyncio.Task[None]]: ...
    @overload
    def emit(self, *, event: Event, event_data: Any) -> list[asyncio.Task[None]]: ...

    def emit(self, *, event: Event, event_data: EventData) -> list[asyncio.Task[None]]:
        """Emit an event with the associated data to all registered listeners.

        Each listener is invoked in its own task, so this method only starts them. It also starts the manager if it
        starts by itself.

        Args:
            event: The event which will be emitted.
            event_data: The data which will be passed to the event listeners.

        Returns:
            The tasks the listeners run in. Waiting for them waits for this emission only, while
            `wait_for_all_listeners_to_complete` waits for every running listener.

        Raises:
            RuntimeError: If called outside a running event loop, or if the manager belongs to another event loop that
                is still running.
        """
        self._start_on_use(asyncio.get_running_loop())

        listener_tasks: list[asyncio.Task[None]] = []

        # No listener can run before this method returns, as it does not await anything. Reading the mapping with
        # `get` keeps the events that nobody listens to out of it.
        for listener, listener_wrappers in self._listeners_to_wrappers.get(event, {}).items():
            task_name = f'Task-{event.value}-{self._get_listener_name(listener)}'

            for listener_wrapper in listener_wrappers:
                listener_task = asyncio.create_task(listener_wrapper(event_data), name=task_name)
                self._listener_tasks[listener_task] = event
                listener_task.add_done_callback(lambda task: self._listener_tasks.pop(task, None))
                listener_tasks.append(listener_task)

        return listener_tasks

    async def wait_for_all_listeners_to_complete(self, *, timeout: timedelta | None = None) -> None:
        """Wait for all currently executing event listeners to complete.

        Args:
            timeout: The maximum time to wait for the event listeners to finish. If they do not complete within
                the specified timeout, they will be canceled. `None` waits without a limit, zero cancels them right
                away.
        """
        await self._wait_for_listeners(timeout=timeout)

    async def _wait_for_listeners(self, *, timeout: timedelta | None, event: Event | None = None) -> None:
        """Wait for the running listeners, only for those of `event` if it's given."""
        # A waiter can't finish until the listeners it awaits do, so waiters must never await each other or
        # themselves - this is what happens when a listener waits or closes from within itself. Only a waiter
        # that is a listener itself can be awaited this way, so only such waiters are tracked and excluded.
        current_task = asyncio.current_task()
        listener_waiter = current_task if current_task in self._listener_tasks else None

        if listener_waiter is not None:
            self._waiting_listener_tasks.add(listener_waiter)
            listener_tasks = [task for task in self._listener_tasks if task not in self._waiting_listener_tasks]
        else:
            # Any other caller is outside the cycle, so it must await every listener, the waiting ones included.
            listener_tasks = list(self._listener_tasks)

        if event is not None:
            listener_tasks = [task for task in listener_tasks if self._listener_tasks[task] is event]

        try:
            await wait_for_all_tasks_to_finish(tasks=listener_tasks, logger=logger, timeout=timeout)
        finally:
            if listener_waiter is not None:
                self._waiting_listener_tasks.discard(listener_waiter)

    def _emit_persist_state_event(self) -> None:
        """Emit a persist state event."""
        self.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))

    def _start_on_use(self, loop: asyncio.AbstractEventLoop) -> None:
        """Move the manager to the event loop it's used from, and start it there if it starts by itself.

        Nothing starts the manager in an event loop that is shutting down.

        A manager that doesn't start by itself has to be entered only for `on`. Its listeners were registered while it
        was active, so they still get the events emitted after the owner stopped it.

        Raises:
            RuntimeError: If the manager belongs to another event loop that is still running.
        """
        self._leave_other_loop(loop)

        if self.supports_lazy_start and not self._active and loop is not self._closed_loop:
            self._start(loop)

    def _leave_other_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Forget the event loop the manager ran in, if it's another one than `loop`.

        Raises:
            RuntimeError: If that event loop is still running.
        """
        if self._loop is None or self._loop is loop:
            return

        if self._loop.is_running():
            raise RuntimeError(f'The {self.__class__.__name__} belongs to another event loop that is still running.')

        # The event loop stopped without shutting the manager down. Unless it's closed, it may resume and run the tasks
        # left in it, so the guard is cancelled here, and the recurring tasks cancel theirs when they start again.
        if not self._loop.is_closed() and self._shutdown_task is not None:
            self._shutdown_task.cancel()

        self._forget_loop()

    def _start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start the recurring work of the manager in the event loop, which is the running one."""
        if self._loop is None:
            self._closed_loop = None
            self._loop = loop
            self._shutdown_task = asyncio.create_task(self._close_with_loop(), name='Task-event-manager-shutdown')

        self._active = True
        self._emit_persist_state_event_rec_task.start()

    async def _stop(self, *, timeout: timedelta | None) -> None:
        """Stop the recurring work, emit the final `PersistState` event and wait for all the running listeners."""
        try:
            # Stop the periodic emission first, so that the event emitted below really is the last one.
            await self._emit_persist_state_event_rec_task.stop()

            # The final event starts once the earlier ones are done, so that a slower earlier save can't land after it.
            await self._wait_for_listeners(timeout=timeout, event=Event.PERSIST_STATE)
            self._emit_persist_state_event()
            await self.wait_for_all_listeners_to_complete(timeout=timeout)
        finally:
            # The manager has to stop even if stopping fails, be it a cancellation or a failed emission.
            self._active = False

    async def _close_with_loop(self) -> None:
        """Wait until cancelled, which `asyncio.run` does when the event loop shuts down, then close the manager.

        The manager stops, if it still runs, and drops the listeners, so that nothing of that event loop outlives it.
        """
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # A guard left in an event loop that the manager moved away from no longer closes it. The garbage collector
            # finalizes a guard left pending by a closed event loop with `GeneratorExit`, which isn't caught here.
            if self._shutdown_task is asyncio.current_task():
                # Set before the stop, so that an owner stopping the manager at the same time can't make the final event
                # below start it again.
                self._closed_loop = asyncio.get_running_loop()

                try:
                    if self._active:
                        timeout = self._SHUTDOWN_TIMEOUT if self._close_timeout is None else self._close_timeout
                        await self._stop(timeout=timeout)
                finally:
                    self._forget_loop()

            raise

    def _forget_loop(self) -> None:
        """Drop everything that belongs to the event loop the manager ran in, its listeners included."""
        self._loop = None
        self._shutdown_task = None
        self._active = False
        self._listeners_to_wrappers.clear()
        self._listener_tasks.clear()
        self._waiting_listener_tasks.clear()

    @classmethod
    def _forget_loops_after_fork(cls) -> None:
        """Reset the managers in a forked child, which inherits the event loop state and listeners of its parent."""
        for event_manager in list(cls._instances):
            # Listeners registered outside an event loop belong to none yet, so the child keeps them.
            if event_manager._loop is not None:  # noqa: SLF001
                event_manager._forget_loop()  # noqa: SLF001

    @staticmethod
    def _get_running_loop() -> asyncio.AbstractEventLoop | None:
        """Get the running event loop, or `None` outside of one."""
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    @staticmethod
    def _wrap_listener(event: Event, listener: EventListener[Any]) -> WrappedListener:
        """Wrap a listener into a coroutine function that invokes it with the emitted event data.

        Whatever depends only on the listener itself is resolved here, once per registration, instead of on every
        invocation. A sync listener is invoked in a separate thread so that it cannot block the event loop, and every
        exception the invocation raises is logged and swallowed - one failing listener must not affect the others.
        """
        is_async = EventManager._is_async_listener(listener)
        takes_event_data = EventManager._takes_event_data(listener)
        listener_name = EventManager._get_listener_name(listener)

        async def listener_wrapper(event_data: EventData) -> None:
            args = (event_data,) if takes_event_data else ()

            try:
                if is_async:
                    await cast('Callable[..., Awaitable[None]]', listener)(*args)
                else:
                    await asyncio.to_thread(cast('Callable[..., None]', listener), *args)
            except Exception:
                logger.exception(
                    'Exception in the event listener',
                    extra={'event_name': event.value, 'listener_name': listener_name},
                )

        return listener_wrapper

    @staticmethod
    def _is_async_listener(listener: EventListener[Any]) -> bool:
        """Check whether calling the listener returns a coroutine that has to be awaited.

        A plain `inspect.iscoroutinefunction` call sees only the object itself, so it reports a class instance with
        an `async def __call__` - a perfectly valid listener - as synchronous. Its `__call__` has to be checked too.
        """
        return inspect.iscoroutinefunction(listener) or inspect.iscoroutinefunction(listener.__call__)

    @staticmethod
    def _takes_event_data(listener: EventListener[Any]) -> bool:
        """Check whether the listener accepts the emitted event data, as a parameterless listener is allowed as well.

        Binding checks the parameters of the signature, not the value bound to them, so anything can stand in for
        the event data here.
        """
        try:
            inspect.signature(listener).bind(None)
        except TypeError:
            return False

        return True

    @staticmethod
    def _get_listener_name(listener: EventListener[Any]) -> str:
        """Get a name of the listener for logging and for naming the tasks it runs in."""
        return getattr(listener, '__name__', None) or type(listener).__name__


# A child forked while a manager runs inherits a copy of its event loop, which still reports that it's running, and
# the listeners of that loop. Without the reset, the first `on()` or `emit()` in the child raises that the manager
# belongs to another event loop. Platforms that can't fork, such as Windows, have no `register_at_fork`.
if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=EventManager._forget_loops_after_fork)  # noqa: SLF001
