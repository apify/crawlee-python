# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/event_manager.ts

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import timedelta
from functools import wraps
from logging import getLogger
from typing import TYPE_CHECKING, TypedDict

from pyee.asyncio import AsyncIOEventEmitter
from typing_extensions import NotRequired

from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.events.types import Event, EventPersistStateData

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events.types import EventData, Listener, WrappedListener

logger = getLogger(__name__)


class EventManagerOptions(TypedDict):
    """Parameter types for subclass __init__ methods, copied from EventManager.__init__."""

    persist_state_interval: NotRequired[timedelta]
    close_timeout: NotRequired[timedelta | None]


class EventManager:
    """Event manager for registering, emitting, and managing event listeners.

    Event manager allows you to register event listeners, emit events, and wait for event listeners to complete
    their execution. It is built on top of the `pyee.asyncio.AsyncIOEventEmitter` class.
    """

    def __init__(
        self,
        *,
        persist_state_interval: timedelta = timedelta(minutes=1),
        close_timeout: timedelta | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            persist_state_interval: Interval at which `PersistState` events are emitted.
            close_timeout: Optional timeout after which the pending event listeners are canceled.
        """
        self._persist_state_interval = persist_state_interval
        self._close_timeout = close_timeout

        # Asynchronous event emitter for handle events and invoke the event listeners.
        self._event_emitter = AsyncIOEventEmitter()

        # Listeners are wrapped inside asyncio.Task. Store their references here so that we can wait for them to finish.
        self._listener_tasks: set[asyncio.Task] = set()

        # Store the mapping between events, listeners and their wrappers in the following way:
        #   event -> listener -> [wrapped_listener_1, wrapped_listener_2, ...]
        self._listeners_to_wrappers: dict[Event, dict[Listener, list[WrappedListener]]] = defaultdict(
            lambda: defaultdict(list),
        )

        # Recurring task for emitting persist state events.
        self._emit_persist_state_event_rec_task = RecurringTask(
            func=self._emit_persist_state_event,
            delay=self._persist_state_interval,
        )

    async def __aenter__(self) -> EventManager:
        """Initializes the event manager upon entering the async context."""
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
        """
        await self.wait_for_all_listeners_to_complete(timeout=self._close_timeout)
        self._event_emitter.remove_all_listeners()
        self._listener_tasks.clear()
        self._listeners_to_wrappers.clear()
        await self._emit_persist_state_event_rec_task.stop()

    def on(self, *, event: Event, listener: Listener) -> None:
        """Add an event listener to the event manager.

        Args:
            event: The Actor event for which to listen to.
            listener: The function (sync or async) which is to be called when the event is emitted.
        """

        @wraps(listener)
        async def listener_wrapper(event_data: EventData) -> None:
            # If the listener is a coroutine function, just call it, otherwise, run it in a separate thread
            # to avoid blocking the event loop
            coro = (
                listener(event_data)
                if asyncio.iscoroutinefunction(listener)
                else asyncio.to_thread(listener, event_data)
            )
            # Note: use `asyncio.iscoroutinefunction` rather then `inspect.iscoroutinefunction` since it works with
            # unittests.mock.AsyncMock. See https://github.com/python/cpython/issues/84753.

            listener_task = asyncio.create_task(coro, name=f'Task-{event.value}-{listener.__name__}')
            self._listener_tasks.add(listener_task)

            try:
                logger.debug('LocalEventManager.on.listener_wrapper(): Awaiting listener task...')
                await listener_task
                logger.debug('LocalEventManager.on.listener_wrapper(): Listener task completed.')
            except Exception:
                # We need to swallow the exception and just log it here, otherwise it could break the event emitter
                logger.exception(
                    'Exception in the event listener',
                    extra={'event_name': event.value, 'listener_name': listener.__name__},
                )
            finally:
                logger.debug('LocalEventManager.on.listener_wrapper(): Removing listener task from the set...')
                self._listener_tasks.remove(listener_task)

        self._listeners_to_wrappers[event][listener].append(listener_wrapper)
        self._event_emitter.add_listener(event.value, listener_wrapper)

    def off(self, *, event: Event, listener: Listener | None = None) -> None:
        """Remove a listener, or all listeners, from an Actor event.

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

    def emit(self, *, event: Event, event_data: EventData) -> None:
        """Emit an event.

        Args:
            event: The event which will be emitted.
            event_data: The data which will be passed to the event listeners.
        """
        self._event_emitter.emit(event.value, event_data)

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
