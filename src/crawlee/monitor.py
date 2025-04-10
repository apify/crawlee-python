from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from os import getloadavg
from typing import TYPE_CHECKING

from psutil import virtual_memory

if TYPE_CHECKING:
    import crawlee._autoscaling as autoscaling
    from crawlee.request_loaders._request_manager import RequestManager
    from crawlee.statistics import Statistics


class Monitor:
    """Periodically display runtime statistics about the crawling process.

    Includes CPU/memory usage, concurrency, and request progress.
    """

    _statistics: Statistics
    _autoscaled_pool: autoscaling.AutoscaledPool
    _request_queue: RequestManager | None
    _task: asyncio.Task[None] | None

    def __init__(
        self,
        statistics: Statistics,
        autoscaled_pool: autoscaling.AutoscaledPool,
        request_queue: RequestManager | None,
    ) -> None:
        """Initialize the Monitor. Sets up monitor state and dependencies."""
        self.statistics = statistics
        self.autoscaled_pool = autoscaled_pool
        self.request_queue = request_queue
        self._task = None
        self._stop_event = asyncio.Event()
        self._monitor_display = MonitorDisplay()

    async def start(self, interval: float = 0.5) -> None:
        """Start the monitor loop."""
        if not self._monitor_display:
            self._monitor_display = MonitorDisplay()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(interval))

    async def stop(self) -> None:
        """Stop the monitor loop."""
        self._stop_event.set()

    async def _run(self, interval: float) -> None:
        """Run the monitor display update at regular intervals."""
        while not self._stop_event.is_set():
            await self.display()
            await asyncio.sleep(interval)

    async def display(self) -> None:
        """Display runtime stats like CPU, memory, and request progress."""
        stats = self.statistics.calculate()
        now = datetime.now(tz=timezone.utc)
        start_time = datetime.now(tz=timezone.utc)

        elapsed = (start_time - now).total_seconds()
        cpu_load = getloadavg()[0]
        memory = virtual_memory()
        mem_load = 1 - memory.available / memory.total

        requests_finished = stats.requests_finished
        requests_failed = stats.requests_failed
        assumed_total = await self.request_queue.get_total_count() if self.request_queue else 0

        if not self._monitor_display:
            raise RuntimeError('Start monitor first!')

        self._monitor_display.log(f'Start: {format_date_time(start_time)}')
        self._monitor_display.log(f'Now: {format_date_time(now)} (running for {elapsed:.1f} secs)')
        self._monitor_display.log(
            f'Progress: {requests_finished} / {assumed_total}'
            f' ({(requests_finished / assumed_total * 100 if assumed_total else 0):.2f}%), '
            f'failed: {requests_failed}'
            f' ({(requests_failed / assumed_total * 100 if assumed_total else 0):.2f}%)'
        )

        self._monitor_display.log(f'Sys. load: {cpu_load:.2f} CPU / {(mem_load * 100):.2f}% Memory')
        self._monitor_display.log(
            f'Concurrencies: Current {self.autoscaled_pool.current_concurrency}, '
            f'Desired {self.autoscaled_pool.desired_concurrency}'
        )


class MonitorDisplay:
    """Helper class to manage and print monitor output to the terminal."""

    def __init__(self, last_lines_count: int = 0, lines_count: int = 0) -> None:
        """Initialize the MonitorDisplay."""
        self.last_lines_count = last_lines_count
        self.lines_count = lines_count

    def log(self, line: str) -> None:
        """Print a line of output to the terminal."""
        if self.lines_count == 0:
            self.lines_count += 1

        str_to_log = str(line)
        sys.stdout.write(f'{str_to_log}\n')
        self.lines_count += 1

    def close(self) -> None:
        """Close the monitor display and clean up terminal output."""
        sys.stdout.write(f'{self.last_lines_count}')


def format_date_time(dt: datetime | int) -> str:
    """Format a datetime or timestamp to a readable string format."""
    date = datetime.fromtimestamp(dt, tz=timezone.utc) if isinstance(dt, int) else dt
    date_str = f'{date.day}/{date.month}/{date.year}'
    time_str = f'{date.hour}:{date.minute}:{date.second}'
    return f'{date_str} {time_str}'


def pad_date(value: int | str, num: int) -> str:
    """Pad a numeric or string value with leading zeroes to reach a target length."""
    string = str(value)
    if len(string) >= num:
        return string

    zeroes_to_add = num - len(string)
    return '0' * zeroes_to_add + string
