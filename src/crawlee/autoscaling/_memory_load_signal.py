from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import WARNING, getLogger
from typing import TYPE_CHECKING

import proclimits
from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.docs import docs_group
from crawlee._utils.log import LoggerOnce
from crawlee._utils.system import MemoryInfo, MemoryUsageInfo, get_memory_info
from crawlee.events._types import Event, EventSystemInfoData

from ._load_signal import LoadSignal, LoadSnapshot, SnapshotStore
from ._types import Ratio

if TYPE_CHECKING:
    from crawlee.events import EventManager

    from ._load_signal import LoadSignalStartContext

logger = getLogger(__name__)
logger_once = LoggerOnce(logger)

SYSTEM_WIDE_MEMORY_OVERLOAD_THRESHOLD = 0.97


@dataclass(kw_only=True)
class MemorySnapshot(LoadSnapshot):
    """A snapshot taken by the `MemoryLoadSignal`."""

    current_size: ByteSize
    """Memory usage of the current Python process and its children."""


@dataclass(frozen=True, kw_only=True)
@docs_group('Autoscaling')
class MemoryLoadSignalOptions:
    """Options of the built-in `MemoryLoadSignal`."""

    overloaded_ratio: float = 0.2
    """The share of the sample window the memory may spend overloaded before the signal reports overload."""


@docs_group('Autoscaling')
class MemoryLoadSignal(LoadSignal):
    """Tracks the memory usage reported by `SYSTEM_INFO` events, and warns when it becomes critical.

    The memory budget is `Configuration.memory_mbytes`, or `Configuration.available_memory_ratio` of the memory
    currently available to the process. The memory counts as overloaded while the process uses more than
    `Configuration.max_used_memory_ratio` of the budget, or while all processes together use more than 97% of the
    memory available to it. The `ConcurrencySystem` builds this signal by default, so construct one only to wrap or
    adapt it.
    """

    name = 'memory_info'
    """The key of the signal in the reported `SystemInfo`."""

    _RESERVE_MEMORY_RATIO = 0.5
    """Fraction of memory kept in reserve. Used to calculate critical memory overload threshold."""

    _MEMORY_WARNING_COOLDOWN_PERIOD = timedelta(milliseconds=10000)
    """Minimum time interval between logging successive critical memory overload warnings."""

    def __init__(self, options: MemoryLoadSignalOptions | None = None) -> None:
        """Initialize a new instance.

        Args:
            options: The options of the signal. Uses the defaults if not provided.
        """
        self._options = options or MemoryLoadSignalOptions()
        self._max_used_ratio = 0.0
        self._store = SnapshotStore[MemorySnapshot]()
        self._max_memory_size: ByteSize | Ratio | None = None
        self._event_manager: EventManager | None = None
        self._timestamp_of_last_memory_warning: datetime = datetime.now(UTC) - timedelta(hours=1)

    @property
    @override
    def overloaded_ratio(self) -> float:
        return self._options.overloaded_ratio

    @override
    async def start(self, context: LoadSignalStartContext) -> None:
        self._store.use_sample_window(context.max_sample_window)
        self._store.clear()

        config = context.configuration

        self._max_used_ratio = config.max_used_memory_ratio

        # Compute the maximum memory size based on the provided configuration. If `memory_mbytes` is provided,
        # it uses that value. Otherwise, it calculates the `max_memory_size` as a proportion of the memory available
        # to this process based on `available_memory_ratio`.
        self._max_memory_size = (
            ByteSize.from_mb(config.memory_mbytes)
            if config.memory_mbytes
            else Ratio(value=config.available_memory_ratio)
        )

        # The default ratio protects the machine from the crawler. Under a limit set outside of Crawlee it stacks on
        # top of that limit, which is rarely what the user meant.
        if not config.memory_mbytes and 'available_memory_ratio' not in config.model_fields_set:
            budget = proclimits.get_memory_budget()
            if budget is not None:
                limit = ByteSize(budget.limit)
                logger_once.log(
                    f'Setting max memory of this run to {limit * config.available_memory_ratio}, '
                    f'{config.available_memory_ratio:.0%} of the {limit} memory limit applying to this process. '
                    'Use the CRAWLEE_MEMORY_MBYTES or CRAWLEE_AVAILABLE_MEMORY_RATIO environment variable to '
                    'override it.',
                    key='default_memory_ratio_under_limit',
                    level=WARNING,
                )

        self._event_manager = service_locator.get_event_manager()
        self._event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot)

    @override
    async def stop(self) -> None:
        if self._event_manager is not None:
            self._event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot)
            self._event_manager = None

    @override
    def get_sample(self, duration: timedelta | None = None) -> list[MemorySnapshot]:
        return self._store.get_sample(duration)

    async def _snapshot(self, event_data: EventSystemInfoData) -> None:
        """Record the memory usage of a `SYSTEM_INFO` event.

        Must be `async`, so that the event manager does not run it in a thread of its own.
        """
        match event_data.memory_info, self._max_memory_size:
            case MemoryInfo() as memory_info, Ratio() as ratio:
                max_memory_size = memory_info.total_size * ratio.value
                system_wide_used_size = memory_info.system_wide_used_size
                system_wide_memory_size = memory_info.total_size

            case MemoryUsageInfo(), Ratio() as ratio:
                # This is just a hypothetical case, that will most likely not happen in practice.
                # `LocalEventManager` should always provide `MemoryInfo` in the event data.
                # When running on Apify, `self._max_memory_size` is always `ByteSize`, not `Ratio`.
                logger_once.log(
                    'It is recommended that a custom implementation of `LocalEventManager` emits `SYSTEM_INFO` events '
                    'with `MemoryInfo` and not just `MemoryUsageInfo`.',
                    key='memory_usage_info_event',
                    level=WARNING,
                )
                max_memory_size = get_memory_info().total_size * ratio.value
                system_wide_used_size = None
                system_wide_memory_size = None

            case MemoryInfo() as memory_info, ByteSize() as byte_size:
                max_memory_size = byte_size
                system_wide_used_size = memory_info.system_wide_used_size
                system_wide_memory_size = memory_info.total_size

            case MemoryUsageInfo(), ByteSize() as byte_size:
                max_memory_size = byte_size
                system_wide_used_size = None
                system_wide_memory_size = None

            case _, _:
                raise NotImplementedError('Unsupported combination of memory info and max memory size types.')

        current_size = event_data.memory_info.current_size
        is_overloaded = current_size / max_memory_size > self._max_used_ratio

        if system_wide_memory_size is not None and system_wide_used_size is not None:
            system_wide_utilization = system_wide_used_size / system_wide_memory_size
            is_overloaded = is_overloaded or system_wide_utilization > SYSTEM_WIDE_MEMORY_OVERLOAD_THRESHOLD

        snapshot = MemorySnapshot(
            current_size=current_size,
            is_overloaded=is_overloaded,
            created_at=event_data.memory_info.created_at,
        )
        self._store.push(snapshot)

        self._evaluate_memory_load(current_size, event_data.memory_info.created_at, max_memory_size=max_memory_size)

    def _evaluate_memory_load(
        self, current_memory_usage_size: ByteSize, snapshot_timestamp: datetime, max_memory_size: ByteSize
    ) -> None:
        """Evaluate and log critical memory load conditions based on the system information.

        Args:
            current_memory_usage_size: The current memory usage.
            snapshot_timestamp: The time at which the memory snapshot was taken.
            max_memory_size: The maximum memory size to be used for evaluation.
        """
        # Check if the warning has been logged recently to avoid spamming
        if snapshot_timestamp < self._timestamp_of_last_memory_warning + self._MEMORY_WARNING_COOLDOWN_PERIOD:
            return

        threshold_memory_size = self._max_used_ratio * max_memory_size
        buffer_memory_size = max_memory_size * (1 - self._max_used_ratio) * self._RESERVE_MEMORY_RATIO
        overload_memory_threshold_size = threshold_memory_size + buffer_memory_size

        # Log a warning if current memory usage exceeds the critical overload threshold
        if current_memory_usage_size > overload_memory_threshold_size:
            memory_usage_percentage = round((current_memory_usage_size.bytes / max_memory_size.bytes) * 100)
            logger.warning(
                f'Memory is critically overloaded. Using {current_memory_usage_size} of '
                f'{max_memory_size} ({memory_usage_percentage}%). '
                'Consider increasing available memory.'
            )
            self._timestamp_of_last_memory_warning = snapshot_timestamp
