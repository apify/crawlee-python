# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/event_manager.ts

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from datetime import timedelta
from functools import wraps
from logging import getLogger
from typing import TYPE_CHECKING, Any, Literal, TypedDict, Union, cast, overload

from pyee.asyncio import AsyncIOEventEmitter

from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.events._types import (
    Event,
    EventAbortingData,
    EventExitData,
    EventListener,
    EventMigratingData,
    EventPersistStateData,
    EventSystemInfoData,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from typing_extensions import NotRequired

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


@docs_group('Classes')
class EventManager:
    """Manage events and their listeners, enabling registration, emission, and execution control.

    It allows for registering event listeners, emitting events, and ensuring all listeners complete their execution.
    Built on top of `pyee.asyncio.AsyncIOEventEmitter`. It implements additional features such as waiting for all
    listeners to complete and emitting `PersistState` events at regular intervals.
    """

    def __init__(
        self,
        *,
        persist_state_interval: timedelta = timedelta(minutes=1),
        close_timeout: timedelta | None = None,
    ) -> None:
        """A default constructor.

        Args:
            persist_state_interval: Interval between emitted `PersistState` events to maintain state persistence.
            close_timeout: Optional timeout for canceling pending event listeners if they exceed this duration.
        """
        self._persist_state_interval = persist_state_interval
        self._close_timeout = close_timeout

        # Asynchronous event emitter for handle events and invoke the event listeners.
        self._event_emitter = AsyncIOEventEmitter()

        # Listeners are wrapped inside asyncio.Task. Store their references here so that we can wait for them to finish.
        self._listener_tasks: set[asyncio.Task] = set()

        # Store the mapping between events, listeners and their wrappers in the following way:
        #   event -> listener -> [wrapped_listener_1, wrapped_listener_2, ...]
        self._listeners_to_wrappers: dict[Event, dict[EventListener[Any], list[WrappedListener]]] = defaultdict(
            lambda: defaultdict(list),
        )

        # Recurring task for emitting persist state events.
        self._emit_persist_state_event_rec_task = RecurringTask(
            func=self._emit_persist_state_event,
            delay=self._persist_state_interval,
        )

        # Flag to indicate the context state.
        self._active = False

    @property
    def active(self) -> bool:
        """Indicate whether the context is active."""
        return self._active

    async def __aenter__(self) -> EventManager:
        """Initializes the event manager upon entering the async context.

        Raises:
            RuntimeError: If the context manager is already active.
        """
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True
        self._emit_persist_state_event_rec_task.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Closes the local event manager upon exiting the async context.

        This will stop listening for the events, and it will wait for all the event listeners to finish.

        Raises:
            RuntimeError: If the context manager is not active.
        """
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        await self.wait_for_all_listeners_to_complete(timeout=self._close_timeout)
        self._event_emitter.remove_all_listeners()
        self._listener_tasks.clear()
        self._listeners_to_wrappers.clear()
        await self._emit_persist_state_event_rec_task.stop()
        self._active = False

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
    def on(self, *, event: Event, listener: EventListener[None]) -> None: ...

    def on(self, *, event: Event, listener: EventListener[Any]) -> None:
        """Register an event listener for a specific event.

        Args:
            event: The event for which to listen to.
            listener: The function (sync or async) which is to be called when the event is emitted.
        """
        signature = inspect.signature(listener)

        @wraps(cast('Callable[..., Union[None, Awaitable[None]]]', listener))
        async def listener_wrapper(event_data: EventData) -> None:
            try:
                bound_args = signature.bind(event_data)
            except TypeError:  # Parameterless listener
                bound_args = signature.bind()

            # If the listener is a coroutine function, just call it, otherwise, run it in a separate thread
            # to avoid blocking the event loop
            coro = (
                listener(*bound_args.args, **bound_args.kwargs)
                if asyncio.iscoroutinefunction(listener)
                else asyncio.to_thread(cast('Callable[..., None]', listener), *bound_args.args, **bound_args.kwargs)
            )
            # Note: use `asyncio.iscoroutinefunction` rather then `inspect.iscoroutinefunction` since it works with
            # unittests.mock.AsyncMock. See https://github.com/python/cpython/issues/84753.

            listener_task = asyncio.create_task(coro, name=f'Task-{event.value}-{listener.__name__}')
            self._listener_tasks.add(listener_task)

            try:
                logger.debug('EventManager.on.listener_wrapper(): Awaiting listener task...')
                await listener_task
                logger.debug('EventManager.on.listener_wrapper(): Listener task completed.')
            except Exception:
                # We need to swallow the exception and just log it here, otherwise it could break the event emitter
                logger.exception(
                    'Exception in the event listener',
                    extra={'event_name': event.value, 'listener_name': listener.__name__},
                )
            finally:
                logger.debug('EventManager.on.listener_wrapper(): Removing listener task from the set...')
                self._listener_tasks.remove(listener_task)

        self._listeners_to_wrappers[event][listener].append(listener_wrapper)
        self._event_emitter.add_listener(event.value, listener_wrapper)

    def off(self, *, event: Event, listener: EventListener[Any] | None = None) -> None:
        """Remove a specific listener or all listeners for an event.

        Args:
            event: The Actor event for which to remove listeners.
            listener: The listener which is supposed to be removed. If not passed, all listeners of this event
                are removed.
        """
        if listener:
            for listener_wrapper in self._listeners_to_wrappers[event][listener]:
                self._event_emitter.remove_listener(event.value, listener_wrapper)
            self._listeners_to_wrappers[event][listener] = []
        else:
            self._listeners_to_wrappers[event] = defaultdict(list)
            self._event_emitter.remove_all_listeners(event.value)

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
    def emit(self, *, event: Event, event_data: Any) -> None: ...

    @ensure_context
    def emit(self, *, event: Event, event_data: EventData) -> None:
        """Emit an event with the associated data to all registered listeners.

        Args:
            event: The event which will be emitted.
            event_data: The data which will be passed to the event listeners.
        """
        self._event_emitter.emit(event.value, event_data)

    @ensure_context
    async def wait_for_all_listeners_to_complete(self, *, timeout: timedelta | None = None) -> None:
        """Wait for all currently executing event listeners to complete.

        Args:
            timeout: The maximum time to wait for the event listeners to finish. If they do not complete within
                the specified timeout, they will be canceled.
        """

        async def wait_for_listeners() -> None:
            """Gathers all listener tasks and awaits their completion, logging any exceptions encountered."""
            results = await asyncio.gather(*self._listener_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.exception('Event listener raised an exception.', exc_info=result)

        tasks = [asyncio.create_task(wait_for_listeners(), name=f'Task-{wait_for_listeners.__name__}')]

        await wait_for_all_tasks_for_finish(tasks=tasks, logger=logger, timeout=timeout)

    async def _emit_persist_state_event(self) -> None:
        """Emits a persist state event with the given migration status."""
        self.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
