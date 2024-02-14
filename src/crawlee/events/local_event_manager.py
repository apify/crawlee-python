# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/events/local_event_manager.ts

from __future__ import annotations

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
    from crawlee import Config

logger = getLogger(__name__)


class LocalEventManager(EventManager):
    """Local event manager for emitting system info events."""

    def __init__(self: LocalEventManager, config: Config) -> None:
        self.config = config
        self._initialized = False
        self._emit_system_info_event_rec_task = RecurringTask(
            func=self._emit_system_info_event,
            delay=self.config.system_info_interval,
        )

        super().__init__()

    async def init(self: LocalEventManager) -> None:
        """Initializes the local event manager."""
        logger.debug('Calling LocalEventManager.init()...')

        if self._initialized:
            raise RuntimeError('LocalEventManager is already initialized.')

        self._emit_system_info_event_rec_task.start()
        self._initialized = True

    async def close(self: LocalEventManager, *, timeout: timedelta | None = None) -> None:
        """Closes the local event manager."""
        logger.debug('Calling LocalEventManager.close()...')

        if not self._initialized:
            raise RuntimeError('LocalEventManager is not initialized.')

        await self._emit_system_info_event_rec_task.stop()
        await super().close(timeout=timeout)
        self._initialized = False

    async def _emit_system_info_event(self: LocalEventManager) -> None:
        """Periodically emits system info events."""
        system_info = await self._create_system_info()
        event_data = EventSystemInfoData(system_info=system_info)
        self.emit(event=Event.SYSTEM_INFO, event_data=event_data)

    async def _create_system_info(self: LocalEventManager) -> SystemInfo:
        """Gathers system info from various metrics."""
        cpu_info = self._get_cpu_info()
        mem_usage = self._get_current_mem_usage()

        return SystemInfo(
            created_at=datetime.now(tz=timezone.utc),
            cpu_info=cpu_info,
            mem_current_bytes=mem_usage,
        )

    def _get_cpu_info(self: LocalEventManager) -> LoadRatioInfo:
        cpu_actual_ratio = psutil.cpu_percent() / 100
        return LoadRatioInfo(
            limit_ratio=self.config.max_used_cpu_ratio,
            actual_ratio=cpu_actual_ratio,
        )

    def _get_current_mem_usage(self: LocalEventManager) -> int:
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
