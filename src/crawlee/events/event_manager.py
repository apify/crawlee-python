from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import suppress
from functools import wraps
from inspect import iscoroutinefunction
from logging import getLogger
from typing import TYPE_CHECKING

from pyee.asyncio import AsyncIOEventEmitter

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.events.types import Event, EventData, Listener, WrappedListener

logger = getLogger(__name__)


class EventManager:
    """Event manager for registering, emitting, and managing event listeners.

    Event manager allows you to register event listeners, emit events, and wait for event listeners to complete
    their execution. It is built on top of the `pyee.asyncio.AsyncIOEventEmitter` class.

    Attributes:
        _event_emitter: The event emitter which is used to emit events and call the event listeners.

        _listener_tasks: The set of asyncio tasks which are currently executing the event listeners.

        _listeners_to_wrappers: The mapping between events and listeners, and the wrapped listeners which are
            registered with the event emitter.
    """

    def __init__(self: EventManager) -> None:
        logger.debug('Calling LocalEventManager.__init__()...')
        self._event_emitter = AsyncIOEventEmitter()

        # Listeners are wrapped in a asyncio.Task, store their references here so that we can wait for them to finish
        self._listener_tasks: set[asyncio.Task] = set()

        # Store the mapping between events and listeners like this:
        #   event -> listener -> [wrapped_listener_1, wrapped_listener_2, ...]
        self._listeners_to_wrappers: dict[Event, dict[Listener, list[WrappedListener]]] = defaultdict(
            lambda: defaultdict(list),
        )

    async def close(self: EventManager, *, timeout: timedelta | None = None) -> None:
        """Close the event manager.

        This will stop listening for the events, and it will wait for all the event listeners to finish.

        Args:
            timeout: Optional timeout after which the pending event listeners are canceled.
        """
        logger.debug('Calling LocalEventManager.close()...')
        await self.wait_for_all_listeners_to_complete(timeout=timeout)
        self._event_emitter.remove_all_listeners()

    def on(self: EventManager, *, event: Event, listener: Listener) -> None:
        """Add an event listener to the event manager.

        Args:
            event: The Actor event for which to listen to.
            listener: The function (sync or async) which is to be called when the event is emitted.
        """
        logger.debug('Calling LocalEventManager.on()...')

        @wraps(listener)
        async def listener_wrapper(event_data: EventData) -> None:
            logger.debug('Calling LocalEventManager.on.listener_wrapper()...')

            # If the listener is a coroutine function, just call it, otherwise, run it in a separate thread
            # to avoid blocking the event loop
            coroutine = (
                listener(event_data) if iscoroutinefunction(listener) else asyncio.to_thread(listener, event_data)
            )

            listener_task = asyncio.create_task(coroutine, name=f'Task-{event.value}-{listener.__name__}')
            self._listener_tasks.add(listener_task)

            try:
                await listener_task
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

    def off(self: EventManager, *, event: Event, listener: Listener | None = None) -> None:
        """Remove a listener, or all listeners, from an Actor event.

        Args:
            event: The Actor event for which to remove listeners.
            listener: The listener which is supposed to be removed. If not passed, all listeners of this event
                are removed.
        """
        logger.debug('Calling LocalEventManager.off()...')

        if listener:
            for listener_wrapper in self._listeners_to_wrappers[event][listener]:
                self._event_emitter.remove_listener(event.value, listener_wrapper)
            self._listeners_to_wrappers[event][listener] = []
        else:
            self._listeners_to_wrappers[event] = defaultdict(list)
            self._event_emitter.remove_all_listeners(event.value)

    def emit(self: EventManager, *, event: Event, event_data: EventData) -> None:
        """Emit an event.

        Args:
            event: The event which will be emitted.
            event_data: The data which will be passed to the event listeners.
        """
        logger.debug('Calling LocalEventManager.emit()...')
        self._event_emitter.emit(event.value, event_data)

    async def wait_for_all_listeners_to_complete(self: EventManager, *, timeout: timedelta | None = None) -> None:
        """Wait for all currently executing event listeners to complete.

        Args:
            timeout: The maximum time to wait for the event listeners to finish. If they do not complete within
                the specified timeout, they will be canceled.
        """
        logger.debug('Calling LocalEventManager.wait_for_all_listeners_to_complete()...')

        async def wait_for_listeners() -> None:
            """Gathers all listener tasks and awaits their completion, logging any exceptions encountered."""
            results = await asyncio.gather(*self._listener_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.exception('Event listener raised an exception.', exc_info=result)

        tasks = [asyncio.create_task(wait_for_listeners(), name=f'Task-{wait_for_listeners.__name__}')]
        timeout_secs = timeout.total_seconds() if timeout else None

        try:
            _, pending = await asyncio.wait(tasks, timeout=timeout_secs)
            if pending:
                logger.warning('Waiting timeout reached; canceling unfinished event listeners.')
        except asyncio.CancelledError:
            logger.warning('Asyncio wait was cancelled; canceling unfinished event listeners.')
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
