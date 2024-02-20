# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/local_event_manager.ts

from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING

import psutil

from crawlee._utils.recurring_task import RecurringTask
from crawlee.autoscaling.types import LoadRatioInfo, SystemInfo
from crawlee.events.event_manager import EventManager
from crawlee.events.types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee import Config

logger = getLogger(__name__)


class LocalEventManager(EventManager):
    """Local event manager for emitting system info events.

    Attributes:
        config: The crawlee configuration.

        timeout: The timeout for closing the event manager.

        _emit_system_info_event_rec_task: The recurring task for emitting system info events.
    """

    def __init__(self: LocalEventManager, config: Config, timeout: timedelta | None = None) -> None:
        self.config = config
        self.timeout = timeout
        self._emit_system_info_event_rec_task = RecurringTask(
            func=self._emit_system_info_event,
            delay=self.config.system_info_interval,
        )
        super().__init__()

    async def __aenter__(self: LocalEventManager) -> LocalEventManager:
        """Initializes the local event manager upon entering the async context."""
        logger.debug('Calling LocalEventManager.__aenter__()...')
        self._emit_system_info_event_rec_task.start()
        return self

    async def __aexit__(
        self: LocalEventManager,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Closes the local event manager upon exiting the async context."""
        logger.debug('Calling LocalEventManager.__aexit__()...')

        if exc_value:
            logger.error('An error occurred while exiting the async context: %s', exc_value)

        await self._emit_system_info_event_rec_task.stop()
        await super().close(timeout=self.timeout)

    async def _emit_system_info_event(self: LocalEventManager) -> None:
        """Emits a system info event with the current CPU and memory usage."""
        system_info = await self._get_system_info()
        event_data = EventSystemInfoData(system_info=system_info)
        self.emit(event=Event.SYSTEM_INFO, event_data=event_data)

    async def _get_system_info(self: LocalEventManager) -> SystemInfo:
        """Gathers system info about the CPU and memory usage.

        Returns:
            The system info.
        """
        cpu_info = await self._get_cpu_info()
        mem_usage = self._get_current_mem_usage()

        return SystemInfo(
            cpu_info=cpu_info,
            mem_current_bytes=mem_usage,
        )

    async def _get_cpu_info(self: LocalEventManager) -> LoadRatioInfo:
        """Retrieves the current CPU usage and calculates the load ratio.

        It utilizes the `psutil` library. Function `psutil.cpu_percent()` returns a float representing the current
        system-wide CPU utilization as a percentage.

        Returns:
            The load ratio info.
        """
        cpu_percent = await asyncio.to_thread(psutil.cpu_percent, interval=0.1)
        cpu_ratio = cpu_percent / 100
        return LoadRatioInfo(limit_ratio=self.config.max_used_cpu_ratio, actual_ratio=cpu_ratio)

    def _get_current_mem_usage(self: LocalEventManager) -> int:
        """Retrieves the current memory usage of the process and its children.

        Returns:
            The current memory usage in bytes.
        """
        current_process = psutil.Process(os.getpid())

        # Retrieve the Resident Set Size (RSS) of the current process. RSS is the portion of memory
        # occupied by a process that is held in RAM.
        mem_bytes = int(current_process.memory_info().rss)

        for child in current_process.children(recursive=True):
            # Ignore any NoSuchProcess exception that might occur if a child process ends before we retrieve
            # its memory usage.
            with suppress(psutil.NoSuchProcess):
                mem_bytes += int(child.memory_info().rss)

        return mem_bytes
