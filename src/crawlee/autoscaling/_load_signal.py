from __future__ import annotations

from bisect import insort
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.configuration import Configuration


@dataclass(kw_only=True)
@docs_group('Autoscaling')
class LoadSnapshot:
    """The overload state of one resource at a point in time."""

    is_overloaded: bool
    """Whether the resource was overloaded."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """The time at which the state was measured."""


@dataclass(frozen=True, kw_only=True)
@docs_group('Autoscaling')
class LoadSignalStartContext:
    """What a `LoadSignal` is told when its `ConcurrencySystem` starts."""

    max_sample_window: timedelta
    """The longest window the signal will be sampled over. Keeping less history narrows what the signal contributes.
    Keeping more only wastes memory."""

    configuration: Configuration
    """The `Configuration` of the `ConcurrencySystem`, which holds the limits of the built-in signals."""


@docs_group('Autoscaling')
class LoadSignal(Protocol):
    """Reports whether one resource is overloaded.

    A `ConcurrencySystem` evaluates all of its signals, and the system counts as overloaded when any of them is. The
    built-in signals watch memory, the event loop, CPU and the storage client's rate-limit errors. To watch something
    of your own, pass any object with these members in `LoadSignalsOptions.custom`, no subclassing needed.
    `SnapshotStore` keeps the snapshots for you.
    """

    @property
    def name(self) -> str:
        """The key of the signal in the reported `SystemInfo`, unique among the signals of one `ConcurrencySystem`.

        The built-in names `memory_info`, `event_loop_info`, `cpu_info` and `storage_client_info` land in the matching
        `SystemInfo` fields, so taking one over requires switching that built-in signal off.
        """

    @property
    def overloaded_ratio(self) -> float:
        """The share of the sample window the resource may spend overloaded before the signal reports overload."""

    async def start(self, context: LoadSignalStartContext) -> None:
        """Start collecting snapshots, retaining at least `context.max_sample_window` of history.

        Called when the `ConcurrencySystem` starts. That may be a restart, so drop anything measured before it.

        Args:
            context: What the signal needs to know about the system that samples it.
        """

    async def stop(self) -> None:
        """Stop collecting snapshots.

        Called when the `ConcurrencySystem` stops, and when this or a later signal fails to start, so it must work even
        if `start` did not finish.
        """

    def get_sample(self, duration: timedelta | None = None) -> Sequence[LoadSnapshot]:
        """Return the snapshots taken within `duration` of the latest one, ordered from the oldest.

        Args:
            duration: How far back to look. All retained snapshots are returned if omitted.
        """


TSnapshot = TypeVar('TSnapshot', bound=LoadSnapshot)


@docs_group('Autoscaling')
class SnapshotStore(Generic[TSnapshot]):
    """Keeps the snapshots of a load signal for the window it is sampled over.

    Snapshots are kept ordered by `created_at`, even when they arrive out of order. All built-in signals keep their
    snapshots in one, and so can a custom `LoadSignal`.
    """

    def __init__(self) -> None:
        self._snapshots: list[TSnapshot] = []
        self._history: timedelta | None = None

    def use_sample_window(self, max_sample_window: timedelta) -> None:
        """Keep only the snapshots within `max_sample_window` of the latest one.

        Call it from `LoadSignal.start` with the window from its context. Until then, nothing is pruned.

        Args:
            max_sample_window: How much history to keep.
        """
        self._history = max_sample_window

    def push(self, snapshot: TSnapshot) -> None:
        """Add a snapshot and drop the ones that fell out of the window.

        Args:
            snapshot: The snapshot to add.
        """
        insort(self._snapshots, snapshot, key=lambda item: item.created_at)

        if self._history is None:
            return

        latest_time = self._snapshots[-1].created_at
        keep_from_index = next(
            index for index, item in enumerate(self._snapshots) if latest_time - item.created_at <= self._history
        )
        del self._snapshots[:keep_from_index]

    def get_sample(self, duration: timedelta | None = None) -> list[TSnapshot]:
        """Return the snapshots taken within `duration` of the latest one.

        Args:
            duration: How far back to look. All snapshots are returned if omitted.
        """
        if duration is None:
            return list(self._snapshots)

        if not self._snapshots:
            return []

        latest_time = self._snapshots[-1].created_at
        return [snapshot for snapshot in self._snapshots if latest_time - snapshot.created_at <= duration]

    def get_all(self) -> list[TSnapshot]:
        """Return all snapshots, for a signal that measures the change since its previous snapshot."""
        return self._snapshots

    def clear(self) -> None:
        """Drop all snapshots.

        The built-in signals do this when they start, so that a restarted signal is not judged on measurements from
        before the downtime.
        """
        self._snapshots = []
