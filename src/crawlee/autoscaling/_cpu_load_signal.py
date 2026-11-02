from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee.events._types import Event

from ._load_signal import LoadSnapshot, SnapshotStore

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.events import EventManager
    from crawlee.events._types import EventSystemInfoData

    from ._load_signal import LoadSignalStartContext


@dataclass(kw_only=True)
class CpuSnapshot(LoadSnapshot):
    """A snapshot taken by the `CpuLoadSignal`."""

    used_ratio: float
    """The ratio of CPU in use."""


@dataclass(frozen=True, kw_only=True)
@docs_group('Autoscaling')
class CpuLoadSignalOptions:
    """Options of the built-in `CpuLoadSignal`."""

    overloaded_ratio: float = 0.4
    """The share of the sample window the CPU may spend overloaded before the signal reports overload."""


@docs_group('Autoscaling')
class CpuLoadSignal:
    """Tracks the CPU usage reported by `SYSTEM_INFO` events.

    The CPU counts as overloaded while its usage is above `Configuration.max_used_cpu_ratio`. The `ConcurrencySystem`
    builds this signal by default, so construct one only to wrap or adapt it.
    """

    name = 'cpu_info'
    """The key of the signal in the reported `SystemInfo`."""

    def __init__(self, options: CpuLoadSignalOptions | None = None) -> None:
        """Initialize a new instance.

        Args:
            options: The options of the signal. Uses the defaults if not provided.
        """
        self._options = options or CpuLoadSignalOptions()
        self._store = SnapshotStore[CpuSnapshot]()
        self._max_used_ratio = 0.0
        self._event_manager: EventManager | None = None

    @property
    def overloaded_ratio(self) -> float:
        """The `overloaded_ratio` from the options of the signal."""
        return self._options.overloaded_ratio

    async def start(self, context: LoadSignalStartContext) -> None:
        """Start recording snapshots, with the limits from `context.configuration`."""
        self._store.use_sample_window(context.max_sample_window)
        self._store.clear()

        self._max_used_ratio = context.configuration.max_used_cpu_ratio

        self._event_manager = service_locator.get_event_manager()
        self._event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot)

    async def stop(self) -> None:
        """Stop recording snapshots."""
        if self._event_manager is not None:
            self._event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot)
            self._event_manager = None

    def get_sample(self, duration: timedelta | None = None) -> list[CpuSnapshot]:
        """Return the snapshots taken within `duration` of the latest one, or all of them if omitted."""
        return self._store.get_sample(duration)

    async def _snapshot(self, event_data: EventSystemInfoData) -> None:
        """Record the CPU usage of a `SYSTEM_INFO` event.

        Must be `async`, so that the event manager does not run it in a thread of its own.
        """
        used_ratio = event_data.cpu_info.used_ratio
        snapshot = CpuSnapshot(
            used_ratio=used_ratio,
            is_overloaded=used_ratio > self._max_used_ratio,
            created_at=event_data.cpu_info.created_at,
        )
        self._store.push(snapshot)
