from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask

from ._load_signal import LoadSnapshot, SnapshotStore

if TYPE_CHECKING:
    from ._load_signal import LoadSignalStartContext


@dataclass(kw_only=True)
class EventLoopSnapshot(LoadSnapshot):
    """A snapshot taken by the `EventLoopLoadSignal`."""

    exceeded: timedelta
    """How much longer than allowed the event loop was blocked."""


@dataclass(frozen=True, kw_only=True)
@docs_group('Autoscaling')
class EventLoopLoadSignalOptions:
    """Options of the built-in `EventLoopLoadSignal`."""

    snapshot_interval: timedelta = timedelta(milliseconds=500)
    """How often the event loop delay is measured."""

    overloaded_ratio: float = 0.6
    """The share of the sample window the event loop may spend overloaded before the signal reports overload."""

    def __post_init__(self) -> None:
        if self.snapshot_interval <= timedelta(0):
            raise ValueError('snapshot_interval must be positive')


@docs_group('Autoscaling')
class EventLoopLoadSignal:
    """Measures how long the event loop is blocked.

    Every `snapshot_interval`, the signal measures how much later than scheduled it got to run, and the event loop
    counts as overloaded while that delay is above `Configuration.max_event_loop_delay`. The `ConcurrencySystem`
    builds this signal by default, so construct one only to wrap or adapt it.
    """

    name = 'event_loop_info'
    """The key of the signal in the reported `SystemInfo`."""

    def __init__(self, options: EventLoopLoadSignalOptions | None = None) -> None:
        """Initialize a new instance.

        Args:
            options: The options of the signal. Uses the defaults if not provided.
        """
        self._options = options or EventLoopLoadSignalOptions()
        self._max_blocked = timedelta(0)
        self._store = SnapshotStore[EventLoopSnapshot]()
        self._snapshot_task = RecurringTask(self._snapshot, self._options.snapshot_interval)

    @property
    def overloaded_ratio(self) -> float:
        """The `overloaded_ratio` from the options of the signal."""
        return self._options.overloaded_ratio

    async def start(self, context: LoadSignalStartContext) -> None:
        """Start recording snapshots, with the limits from `context.configuration`."""
        self._store.use_sample_window(context.max_sample_window)
        # Otherwise the first measurement charges the downtime to the event loop.
        self._store.clear()

        self._max_blocked = context.configuration.max_event_loop_delay

        self._snapshot_task.start()

    async def stop(self) -> None:
        """Stop recording snapshots."""
        await self._snapshot_task.stop()

    def get_sample(self, duration: timedelta | None = None) -> list[EventLoopSnapshot]:
        """Return the snapshots taken within `duration` of the latest one, or all of them if omitted."""
        return self._store.get_sample(duration)

    def _snapshot(self) -> None:
        """Record how long the event loop was blocked since the previous snapshot."""
        now = datetime.now(UTC)
        snapshots = self._store.get_sample()
        delay = now - snapshots[-1].created_at - self._options.snapshot_interval if snapshots else timedelta(0)

        snapshot = EventLoopSnapshot(
            exceeded=max(delay - self._max_blocked, timedelta(0)),
            is_overloaded=delay > self._max_blocked,
            created_at=now,
        )
        self._store.push(snapshot)
