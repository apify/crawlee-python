# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/local_event_manager.ts

from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

import psutil

from crawlee._utils.recurring_task import RecurringTask
from crawlee.autoscaling.types import CpuInfo, MemoryInfo
from crawlee.events.event_manager import EventManager
from crawlee.events.types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from types import TracebackType

logger = getLogger(__name__)


class LocalEventManager(EventManager):
    """Local event manager for emitting system info events.

    Attributes:
        system_info_interval: Interval at which `SystemInfo` events are emitted.

        timeout: Optional timeout for closing the event manager.

        _emit_system_info_event_rec_task: Recurring task for emitting system info events.
    """

    def __init__(
        self: LocalEventManager,
        system_info_interval: timedelta = timedelta(seconds=60),
        timeout: timedelta | None = None,
    ) -> None:
        self.system_info_interval = system_info_interval
        self.timeout = timeout
        self._emit_system_info_event_rec_task: RecurringTask | None = None
        super().__init__()

    async def __aenter__(self) -> LocalEventManager:
        """Initializes the local event manager upon entering the async context.

        It starts emitting system info events at regular intervals.
        """
        logger.debug('Calling LocalEventManager.__aenter__()...')
        self._emit_system_info_event_rec_task = RecurringTask(
            func=self._emit_system_info_event,
            delay=self.system_info_interval,
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

        await super().close(timeout=self.timeout)

    async def _emit_system_info_event(self) -> None:
        """Emits a system info event with the current CPU and memory usage."""
        logger.debug('Calling LocalEventManager._emit_system_info_event()...')

        cpu_info = await self._get_cpu_info()
        memory_info = self._get_memory_info()

        event_data = EventSystemInfoData(cpu_info=cpu_info, memory_info=memory_info)
        self.emit(event=Event.SYSTEM_INFO, event_data=event_data)

    async def _get_cpu_info(self: LocalEventManager) -> CpuInfo:
        """Retrieves the current CPU usage.

        It utilizes the `psutil` library. Function `psutil.cpu_percent()` returns a float representing the current
        system-wide CPU utilization as a percentage.
        """
        logger.debug('Calling LocalEventManager._get_cpu_info()...')
        cpu_percent = await asyncio.to_thread(psutil.cpu_percent, interval=0.1)
        current_usage_ratio = cpu_percent / 100
        return CpuInfo(current_usage_ratio=current_usage_ratio)

    def _get_memory_info(self: LocalEventManager) -> MemoryInfo:
        """Retrieves the current memory usage of the process and its children.

        It utilizes the `psutil` library.
        """
        logger.debug('Calling LocalEventManager._get_memory_info()...')
        current_process = psutil.Process(os.getpid())

        # Retrieve the Resident Set Size (RSS) of the current process. RSS is the portion of memory
        # occupied by a process that is held in RAM.
        current_process_bytes = int(current_process.memory_info().rss)

        child_processes_bytes = 0
        for child in current_process.children(recursive=True):
            # Ignore any NoSuchProcess exception that might occur if a child process ends before we retrieve
            # its memory usage.
            with suppress(psutil.NoSuchProcess):
                child_processes_bytes += int(child.memory_info().rss)

        virtual_memory = psutil.virtual_memory()

        return MemoryInfo(
            total_bytes=virtual_memory.total,
            available_bytes=virtual_memory.available,
            used_bytes=virtual_memory.used,
            current_process_bytes=current_process_bytes,
            child_processes_bytes=child_processes_bytes,
        )
