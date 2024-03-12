# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/local_event_manager.ts

from __future__ import annotations

import asyncio
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.system import get_cpu_info, get_memory_info
from crawlee.events.event_manager import EventManager
from crawlee.events.types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from types import TracebackType

logger = getLogger(__name__)


class LocalEventManager(EventManager):
    """Local event manager for emitting system info events."""

    def __init__(
        self,
        *,
        system_info_interval: timedelta = timedelta(seconds=60),
        close_timeout: timedelta | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            system_info_interval: Interval at which `SystemInfo` events are emitted.
            close_timeout: Optional timeout for closing the event manager.
        """
        logger.debug('Calling LocalEventManager.__init__()...')

        self._system_info_interval = system_info_interval
        self._close_timeout = close_timeout

        # Recurring task for emitting system info events.
        self._emit_system_info_event_rec_task: RecurringTask | None = None

        super().__init__()

    async def __aenter__(self) -> LocalEventManager:
        """Initializes the local event manager upon entering the async context.

        It starts emitting system info events at regular intervals.
        """
        logger.debug('Calling LocalEventManager.__aenter__()...')
        self._emit_system_info_event_rec_task = RecurringTask(
            func=self._emit_system_info_event,
            delay=self._system_info_interval,
        )
        self._emit_system_info_event_rec_task.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Closes the local event manager upon exiting the async context.

        It stops emitting system info events and closes the event manager.
        """
        logger.debug('Calling LocalEventManager.__aexit__()...')

        if exc_value:
            logger.error('An error occurred while exiting the async context: %s', exc_value)

        if self._emit_system_info_event_rec_task is not None:
            await self._emit_system_info_event_rec_task.stop()

        await super().close(timeout=self._close_timeout)

    async def _emit_system_info_event(self) -> None:
        """Emits a system info event with the current CPU and memory usage."""
        logger.debug('Calling LocalEventManager._emit_system_info_event()...')

        cpu_info = await asyncio.to_thread(get_cpu_info)
        memory_info = await asyncio.to_thread(get_memory_info)

        event_data = EventSystemInfoData(cpu_info=cpu_info, memory_info=memory_info)
        self.emit(event=Event.SYSTEM_INFO, event_data=event_data)
