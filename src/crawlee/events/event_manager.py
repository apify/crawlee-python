from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import suppress
from functools import wraps
from inspect import iscoroutinefunction
from logging import getLogger
from typing import TYPE_CHECKING, Any

from pyee.asyncio import AsyncIOEventEmitter

if TYPE_CHECKING:
    from crawlee.events.types import Event, Listener, WrappedListener

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

    async def close(self: EventManager, *, timeout_secs: int | None = None) -> None:
        """Close the event manager.

        This will stop listening for the events, and it will wait for all the event listeners to finish.

        Args:
            timeout_secs: Optional timeout after which the pending event listeners are canceled.
        """
        logger.debug('Calling LocalEventManager.close()...')
        await self.wait_for_all_listeners_to_complete(timeout_secs=timeout_secs)
        self._event_emitter.remove_all_listeners()

    def on(self: EventManager, *, event: Event, listener: Listener) -> None:
        """Add an event listener to the event manager.

        Args:
            event: The Actor event for which to listen to.
            listener: The function (sync or async) which is to be called when the event is emitted.
        """
        logger.debug('Calling LocalEventManager.on()...')

        @wraps(listener)
        async def listener_wrapper(*args: Any, **kwargs: Any) -> None:
            logger.debug('Calling LocalEventManager.on.listener_wrapper()...')

            if iscoroutinefunction(listener):
                logger.debug('LocalEventManager.on.listener_wrapper(): Listener is a coroutine function...')

                listener_task = asyncio.create_task(
                    listener(*args, **kwargs),
                    name=f'Task-{event.value}-{listener.__name__}',
                )
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

            else:
                logger.debug('LocalEventManager.on.listener_wrapper(): Listener is a regular function...')
                listener(*args, **kwargs)

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

    def emit(self: EventManager, event: Event, *args: Any, **kwargs: Any) -> None:
        """Emit an event.

        Args:
            event: The event which will be emitted.
            *args: The positional arguments which will be passed to the event listeners.
            **kwargs: The keyword arguments which will be passed to the event listeners.
        """
        logger.debug('Calling LocalEventManager.emit()...')
        self._event_emitter.emit(event.value, *args, **kwargs)

    async def wait_for_all_listeners_to_complete(self: EventManager, *, timeout_secs: float | None = None) -> None:
        """Wait for all event listeners which are currently being executed to complete.

        Args:
            timeout_secs: Timeout for the wait. If the event listeners don't finish until the timeout,
                they will be canceled.
        """
        logger.debug('Calling LocalEventManager.wait_for_all_listeners_to_complete()...')

        async def _wait_for_listeners() -> None:
            logger.debug('LocalEventManager.wait_for_all_listeners_to_complete(): inner _wait_for_listeners() called')
            results = await asyncio.gather(*self._listener_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.exception(
                        'Event manager encountered an exception in one of the event listeners', exc_info=result
                    )

        with suppress(asyncio.CancelledError):
            _, pending = await asyncio.wait(
                [
                    asyncio.create_task(
                        coro=_wait_for_listeners(),
                        name=f'Task-{_wait_for_listeners.__name__}',
                    )
                ],
                timeout=timeout_secs,
            )

            ts = asyncio.all_tasks()
            logger.info(f'ts={ts}')

            if pending:
                logger.warning(
                    'Timed out waiting for event listeners to complete, unfinished event listeners will be canceled'
                )
                for pending_task in pending:
                    pending_task.cancel()
                    await pending_task
