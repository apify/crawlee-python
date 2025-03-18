# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/local_event_manager.ts

from __future__ import annotations

import asyncio
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.system import get_cpu_info, get_memory_info
from crawlee.configuration import Configuration
from crawlee.events._event_manager import EventManager, EventManagerOptions
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Unpack

logger = getLogger(__name__)


@docs_group('Classes')
class LocalEventManager(EventManager):
    """Event manager for local environments.

    It extends the `EventManager` to emit `SystemInfo` events at regular intervals. The `LocalEventManager`
    is intended to be used in local environments, where the system metrics are required managing the `Snapshotter`
    and `AutoscaledPool`.
    """

    def __init__(
        self,
        system_info_interval: timedelta = timedelta(seconds=1),
        **event_manager_options: Unpack[EventManagerOptions],
    ) -> None:
        """Initialize a new instance.

        In most cases, you should use the `from_config` constructor to create a new instance based on
        the provided configuration.

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

    @classmethod
    def from_config(cls, config: Configuration | None = None) -> LocalEventManager:
        """Initialize a new instance based on the provided `Configuration`.

        Args:
            config: The `Configuration` instance. Uses the global (default) one if not provided.
        """
        config = config or Configuration.get_global_configuration()

        return cls(
            system_info_interval=config.system_info_interval,
            persist_state_interval=config.persist_state_interval,
        )

    async def __aenter__(self) -> LocalEventManager:
        """Initialize the local event manager upon entering the async context.

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
        """Close the local event manager upon exiting the async context.

        It stops emitting system info events and closes the event manager.
        """
        await self._emit_system_info_event_rec_task.stop()
        await super().__aexit__(exc_type, exc_value, exc_traceback)

    async def _emit_system_info_event(self) -> None:
        """Emit a system info event with the current CPU and memory usage."""
        cpu_info = await asyncio.to_thread(get_cpu_info)
        memory_info = await asyncio.to_thread(get_memory_info)

        event_data = EventSystemInfoData(cpu_info=cpu_info, memory_info=memory_info)
        self.emit(event=Event.SYSTEM_INFO, event_data=event_data)
