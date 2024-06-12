# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/local_event_manager.ts

from __future__ import annotations

import asyncio
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from typing_extensions import Unpack

from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.system import get_cpu_info, get_memory_info
from crawlee.events.event_manager import EventManager, EventManagerOptions
from crawlee.events.types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from types import TracebackType

logger = getLogger(__name__)


class LocalEventManager(EventManager):
    """Local event manager for emitting system info events."""

    def __init__(
        self,
        system_info_interval: timedelta = timedelta(seconds=1),
        **event_manager_options: Unpack[EventManagerOptions],
    ) -> None:
        """Create a new instance.

        Args:
            system_info_interval: Interval at which `SystemInfo` events are emitted.
            event_manager_options: Additional options for the parent class.
        """
        self._system_info_interval = system_info_interval

        # Recurring task for emitting system info events.
        self._emit_system_info_event_rec_task = RecurringTask(
            func=self._emit_system_info_event,
            delay=self._system_info_interval,
        )

        super().__init__(**event_manager_options)

    async def __aenter__(self) -> LocalEventManager:
        """Initializes the local event manager upon entering the async context.

        It starts emitting system info events at regular intervals.
        """
        await super().__aenter__()
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
        await self._emit_system_info_event_rec_task.stop()
        await super().__aexit__(exc_type, exc_value, exc_traceback)

    async def _emit_system_info_event(self) -> None:
        """Emits a system info event with the current CPU and memory usage."""
        cpu_info = await asyncio.to_thread(get_cpu_info)
        memory_info = await asyncio.to_thread(get_memory_info)

        event_data = EventSystemInfoData(cpu_info=cpu_info, memory_info=memory_info)
        self.emit(event=Event.SYSTEM_INFO, event_data=event_data)
