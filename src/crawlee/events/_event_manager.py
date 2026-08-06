from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast, overload

from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
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

    from typing_extensions import NotRequired, Self

    from crawlee.events._types import EventData, WrappedListener

logger = getLogger(__name__)


class EventManagerOptions(TypedDict):
    """Arguments for the `EventManager` constructor.

    It is intended for typing forwarded `__init__` arguments in the subclasses.
    """

    persist_state_interval: NotRequired[timedelta]
    """Interval between emitted `PersistState` events to maintain state persistence."""

    close_timeout: NotRequired[timedelta | None]
    """Optional timeout for canceling pending event listeners if they exceed this duration."""


@docs_group('Event managers')
class EventManager:
    """Manage events and their listeners, enabling registration, emission, and execution control.

    Listeners can be registered for any of the events and are invoked whenever the event is emitted, each of them
    in its own task - the sync ones in a separate thread so that they cannot block the event loop. On top of that,
    it emits `PersistState` events at regular intervals and can wait for all the running listeners to complete.
    """

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
        """
        self._persist_state_interval = persist_state_interval
        """Interval between the emitted `PersistState` events."""

        self._close_timeout = close_timeout
        """Timeout for the pending listeners when the context is being left, unlimited if not set."""

        self._listener_tasks: set[asyncio.Task[None]] = set()
        """Tasks of the running listener invocations, kept both to keep them alive and to be able to wait for them."""

        self._waiting_listener_tasks: set[asyncio.Task[None]] = set()
        """Listener tasks currently blocked in `wait_for_all_listeners_to_complete`.

        They are excluded from the waits made by the listeners themselves, as a waiter must never await itself.
        """

        self._listeners_to_wrappers: dict[Event, dict[EventListener[Any], list[WrappedListener]]] = defaultdict(
            lambda: defaultdict(list),
        )
        """Registered listeners and their wrappers, mapped as `event -> listener -> [wrapper_1, wrapper_2, ...]`."""

        self._emit_persist_state_event_rec_task = RecurringTask(
            func=self._emit_persist_state_event,
            delay=self._persist_state_interval,
        )
        """Recurring task emitting the `PersistState` events."""

        self._active_ref_count = 0
        """Reference count of the active contexts."""

    @property
    def active(self) -> bool:
        """Indicate whether the context is active."""
        return self._active_ref_count > 0

    async def __aenter__(self) -> Self:
        """Initialize the event manager upon entering the async context."""
        self._active_ref_count += 1

        if self._active_ref_count == 1:
            self._emit_persist_state_event_rec_task.start()

        return self

    @ensure_context
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Close the event manager upon exiting the async context.

        This will stop listening for the events, and it will wait for all the event listeners to finish.

        Raises:
            RuntimeError: If the context manager is not active.
        """
        last_exit = self._active_ref_count == 1

        try:
            # Stop the periodic emission first, so that the event emitted below really is the last one.
            if last_exit:
                await self._emit_persist_state_event_rec_task.stop()

            # Emit a persist state event to ensure the latest state is saved before closing the context.
            await self._emit_persist_state_event()
            await self.wait_for_all_listeners_to_complete(timeout=self._close_timeout)
        finally:
            # The context has to be left even if closing fails, be it a cancellation or a failed emission. Staying
            # active would mean never emitting `PersistState` again, as re-entering the context would be a no-op.
            if last_exit:
                self._listener_tasks.clear()
                self._listeners_to_wrappers.clear()
                self._waiting_listener_tasks.clear()

            self._active_ref_count -= 1

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

        Args:
            event: The event for which to listen to.
            listener: The function (sync or async) which is to be called when the event is emitted.
        """
        self._listeners_to_wrappers[event][listener].append(self._wrap_listener(event, listener))

    def off(self, *, event: Event, listener: EventListener[Any] | None = None) -> None:
        """Remove a specific listener or all listeners for an event.

        Args:
            event: The Actor event for which to remove listeners.
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
    def emit(self, *, event: Literal[Event.PERSIST_STATE], event_data: EventPersistStateData) -> None: ...
    @overload
    def emit(self, *, event: Literal[Event.SYSTEM_INFO], event_data: EventSystemInfoData) -> None: ...
    @overload
    def emit(self, *, event: Literal[Event.MIGRATING], event_data: EventMigratingData) -> None: ...
    @overload
    def emit(self, *, event: Literal[Event.ABORTING], event_data: EventAbortingData) -> None: ...
    @overload
    def emit(self, *, event: Literal[Event.EXIT], event_data: EventExitData) -> None: ...
    @overload
    def emit(self, *, event: Literal[Event.CRAWLER_STATUS], event_data: EventCrawlerStatusData) -> None: ...
    @overload
    def emit(self, *, event: Event, event_data: Any) -> None: ...

    @ensure_context
    def emit(self, *, event: Event, event_data: EventData) -> None:
        """Emit an event with the associated data to all registered listeners.

        Each listener is invoked in its own task, so this method only starts them. Use
        `wait_for_all_listeners_to_complete` to wait for them to finish.

        Args:
            event: The event which will be emitted.
            event_data: The data which will be passed to the event listeners.
        """
        # No listener can run before this method returns, as it does not await anything. Reading the mapping with
        # `get` keeps the events that nobody listens to out of it.
        for listener, listener_wrappers in self._listeners_to_wrappers.get(event, {}).items():
            task_name = f'Task-{event.value}-{self._get_listener_name(listener)}'

            for listener_wrapper in listener_wrappers:
                listener_task = asyncio.create_task(listener_wrapper(event_data), name=task_name)
                self._listener_tasks.add(listener_task)
                listener_task.add_done_callback(self._listener_tasks.discard)

    @ensure_context
    async def wait_for_all_listeners_to_complete(self, *, timeout: timedelta | None = None) -> None:
        """Wait for all currently executing event listeners to complete.

        Args:
            timeout: The maximum time to wait for the event listeners to finish. If they do not complete within
                the specified timeout, they will be canceled.
        """
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

        try:
            await wait_for_all_tasks_to_finish(tasks=listener_tasks, logger=logger, timeout=timeout)
        finally:
            if listener_waiter is not None:
                self._waiting_listener_tasks.discard(listener_waiter)

    async def _emit_persist_state_event(self) -> None:
        """Emit a persist state event with the given migration status."""
        self.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))

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
