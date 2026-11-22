from __future__ import annotations

import asyncio
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

import proclimits
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.system import get_cpu_info, get_memory_info
from crawlee.configuration import Configuration
from crawlee.events._event_manager import EventManager, EventManagerOptions
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from typing import Unpack

logger = getLogger(__name__)


@docs_group('Event managers')
class LocalEventManager(EventManager):
    """Event manager for local environments.

    It extends the `EventManager` to emit `SystemInfo` events at regular intervals while anything listens to them.
    The `LocalEventManager` is intended to be used in local environments, where the system metrics are required by the
    load signals of the `ConcurrencySystem`.
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
        super().__init__(**event_manager_options)

        self._system_info_interval = system_info_interval
        """Interval between the emitted `SystemInfo` events."""

        self._cpu_load = proclimits.CpuLoad()
        """CPU sampler of this event manager, measuring across the gap between its emissions."""

        self._emit_system_info_event_rec_task = RecurringTask(
            func=self._emit_system_info_event,
            delay=self._system_info_interval,
        )
        """Recurring task emitting the `SystemInfo` events."""

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

    @override
    def _start(self, loop: asyncio.AbstractEventLoop) -> None:
        super()._start(loop)

        # A reading kept from a previous run would report the average load over the idle gap since then.
        self._cpu_load = proclimits.CpuLoad()
        self._emit_system_info_event_rec_task.start()

    @override
    async def _stop(self, *, timeout: timedelta | None) -> None:
        try:
            await self._emit_system_info_event_rec_task.stop()
        finally:
            await super()._stop(timeout=timeout)

    async def _emit_system_info_event(self) -> None:
        """Emit a system info event with the current CPU and memory usage, if anyone listens to it."""
        # The manager runs for as long as its event loop does, so it measures only while the readings are needed.
        if not self._listeners_to_wrappers.get(Event.SYSTEM_INFO):
            # A fresh sampler, so that the first reading for a new listener doesn't span the idle gap.
            self._cpu_load = proclimits.CpuLoad()
            return

        # Both readings block the thread they run in - `get_cpu_info` may sample the CPU utilization over a short
        # interval - so run them concurrently instead of one after the other.
        cpu_info, memory_info = await asyncio.gather(
            asyncio.to_thread(get_cpu_info, self._cpu_load),
            asyncio.to_thread(get_memory_info),
        )

        event_data = EventSystemInfoData(cpu_info=cpu_info, memory_info=memory_info)
        self.emit(event=Event.SYSTEM_INFO, event_data=event_data)
