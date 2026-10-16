from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Protocol

from pydantic import Field
from pydantic.dataclasses import dataclass as pydantic_dataclass

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from crawlee._utils.byte_size import ByteSize

SYSTEM_WIDE_MEMORY_OVERLOAD_THRESHOLD = 0.97


@dataclass
@docs_group('Autoscaling')
class LoadRatioInfo:
    """Represent the load ratio of a resource."""

    limit_ratio: float
    """The maximum ratio of overloaded and non-overloaded samples. If the actual ratio exceeds this value,
    the resource is considered as overloaded."""

    actual_ratio: float
    """The actual ratio of overloaded and non-overloaded samples."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the resource is currently overloaded."""
        return self.actual_ratio > self.limit_ratio


@dataclass
@docs_group('Autoscaling')
class SystemInfo:
    """Represent the current status of the system."""

    cpu_info: LoadRatioInfo
    """The CPU load ratio."""

    memory_info: LoadRatioInfo
    """The memory load ratio."""

    event_loop_info: LoadRatioInfo
    """The event loop load ratio."""

    client_info: LoadRatioInfo
    """The client load ratio."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """The time at which the system load information was measured."""

    @property
    def is_system_idle(self) -> bool:
        """Indicate whether the system is currently idle or overloaded."""
        return (
            not self.cpu_info.is_overloaded
            and not self.memory_info.is_overloaded
            and not self.event_loop_info.is_overloaded
            and not self.client_info.is_overloaded
        )

    def __str__(self) -> str:
        """Name the overloaded resources with the share of time each spent over its threshold, against its limit."""
        resources = {
            'cpu': self.cpu_info,
            'memory': self.memory_info,
            'event loop': self.event_loop_info,
            'client': self.client_info,
        }
        overloaded = [
            f'{name} {info.actual_ratio:.0%} of the time (limit {info.limit_ratio:.0%})'
            for name, info in resources.items()
            if info.is_overloaded
        ]
        return f'overloaded: {", ".join(overloaded)}' if overloaded else 'system idle'


@dataclass
class CpuSnapshot:
    """A snapshot of CPU usage."""

    used_ratio: float
    """The ratio of CPU currently in use."""

    max_used_ratio: float
    """The maximum ratio of CPU that is considered acceptable."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """The time at which the system load information was measured."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the CPU is considered as overloaded."""
        return self.used_ratio > self.max_used_ratio


@dataclass
class MemorySnapshot:
    """A snapshot of memory usage."""

    current_size: ByteSize
    """Memory usage of the current Python process and its children."""

    system_wide_used_size: ByteSize | None
    """Memory usage of all processes, within the scope `system_wide_memory_size` covers."""

    max_memory_size: ByteSize
    """The maximum memory that can be used by the crawlers this snapshot is measured for."""

    system_wide_memory_size: ByteSize | None
    """Total memory available to this process, which is the memory limit where one applies."""

    max_used_memory_ratio: float
    """The maximum acceptable ratio of `current_size` to `max_memory_size`."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """The time at which the system load information was measured."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the memory is considered as overloaded."""
        if self.system_wide_memory_size is not None and self.system_wide_used_size is not None:
            system_wide_utilization = self.system_wide_used_size / self.system_wide_memory_size
            if system_wide_utilization > SYSTEM_WIDE_MEMORY_OVERLOAD_THRESHOLD:
                return True

        return (self.current_size / self.max_memory_size) > self.max_used_memory_ratio


@dataclass
class EventLoopSnapshot:
    """Snapshot of the state of the event loop."""

    delay: timedelta
    """The current delay of the event loop."""

    max_delay: timedelta
    """The maximum delay that is considered acceptable."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """The time at which the system load information was measured."""

    @property
    def max_delay_exceeded(self) -> timedelta:
        """The amount of time by which the delay exceeds the maximum delay."""
        return max(self.delay - self.max_delay, timedelta(seconds=0))

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the event loop is considered as overloaded."""
        return self.delay > self.max_delay


@dataclass
class ClientSnapshot:
    """Snapshot of the state of the client."""

    error_count: int
    """The number of errors (HTTP 429) that occurred."""

    new_error_count: int
    """The number of new errors (HTTP 429) that occurred since the last snapshot."""

    max_error_count: int
    """The maximum number of errors that is considered acceptable."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """The time at which the system load information was measured."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the client is considered as overloaded."""
        return self.new_error_count > self.max_error_count


Snapshot = MemorySnapshot | CpuSnapshot | EventLoopSnapshot | ClientSnapshot


@pydantic_dataclass
class Ratio:
    """Represents ratio of memory."""

    value: Annotated[float, Field(gt=0.0, le=1.0)]


@dataclass(frozen=True)
@docs_group('Autoscaling')
class ConcurrencyConsumer:
    """Identifies who is asking a concurrency system for capacity.

    The same instance is passed on every call an `AutoscaledPool` makes, so a system that allocates capacity per
    consumer can key its state off it.
    """

    id: str
    """Process-unique and human-readable identifier of the consumer."""


@docs_group('Autoscaling')
class ConcurrencySystemInterface(Protocol):
    """The contract between an `AutoscaledPool` and the component that governs its concurrency.

    The system answers whether there is free compute for one more task and tracks the budget that tasks are booked
    against. `ConcurrencySystem` is the default implementation. Anything matching this contract can take its place,
    without depending on its internals.

    Every allocation method is told which `ConcurrencyConsumer` is asking, so an implementation may allocate per
    consumer. `ConcurrencySystem` does not, and serves whoever asks first, which can starve a consumer that joins
    a saturated system late.
    """

    @property
    def desired_concurrency(self) -> int:
        """The number of tasks that should currently be running in parallel, given a sufficient supply of them.

        Must always be at least 1, otherwise a pool could never start its first task.
        """

    @property
    def current_concurrency(self) -> int:
        """The number of parallel tasks currently booked against this system."""

    @property
    def active(self) -> bool:
        """Whether the system is ready to be booked against. `AutoscaledPool.run` refuses to run when it is not."""

    def has_capacity_for_task(self, consumer: ConcurrencyConsumer) -> bool:
        """Report whether one more task may start right now on behalf of `consumer`.

        A cheap pre-check the pool consults before querying task readiness. It must not enforce rate limits that only
        make sense for ready tasks, such as a per-minute task cap, because the pool calls this before knowing whether
        any task is ready.

        It should not refuse a `consumer` that has nothing in flight, otherwise its pool stalls with work still pending.

        Args:
            consumer: The consumer asking for capacity.
        """

    def try_register_task_start(self, consumer: ConcurrencyConsumer, task_id: str) -> bool:
        """Book a task against the budget for `consumer`, or report that there is no room for it.

        Must be an atomic (synchronous) check-and-book. Several pools may share one system, and a check separated from
        the booking by an `await` lets two of them claim the last free slot at once.

        Args:
            consumer: The consumer the task is booked for.
            task_id: Identifies the task among the ones `consumer` has in flight, and is repeated by
                `register_task_end`. It is unique only per consumer and may be reused once its task has ended.

        Returns:
            `True` if the task was booked, `False` if there was no room for it.
        """

    def register_task_end(self, consumer: ConcurrencyConsumer, task_id: str) -> None:
        """Return a task's slot to `consumer`'s budget. Called once the task settles, whether it succeeded or failed.

        Args:
            consumer: The consumer the task was booked for.
            task_id: The id the task was booked under.
        """
