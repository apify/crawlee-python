from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Protocol

from pydantic import Field
from pydantic.dataclasses import dataclass as pydantic_dataclass

from crawlee._utils.docs import docs_group


@dataclass
@docs_group('Autoscaling')
class LoadRatioInfo:
    """Represent the load ratio of a resource."""

    limit_ratio: float
    """The share of the sample window the resource may spend overloaded. Above it, the resource counts as overloaded."""

    actual_ratio: float
    """The share of the sample window the resource spent overloaded, weighted by the time between snapshots."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the resource is currently overloaded."""
        return self.actual_ratio > self.limit_ratio


@dataclass
@docs_group('Autoscaling')
class SystemInfo:
    """Represent the current status of the system.

    A built-in load signal that was switched off is reported with zero ratios, so it never counts as overloaded.
    """

    cpu_info: LoadRatioInfo
    """The CPU load ratio."""

    memory_info: LoadRatioInfo
    """The memory load ratio."""

    event_loop_info: LoadRatioInfo
    """The event loop load ratio."""

    storage_client_info: LoadRatioInfo
    """The storage client load ratio."""

    load_signal_info: dict[str, LoadRatioInfo] = field(default_factory=dict)
    """The load ratios of the custom load signals, keyed by signal name. A custom signal named after a built-in one
    is reported in that built-in field instead."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """The time at which the system load information was measured."""

    @property
    def is_system_idle(self) -> bool:
        """Indicate whether the system is currently idle or overloaded."""
        return (
            not self.cpu_info.is_overloaded
            and not self.memory_info.is_overloaded
            and not self.event_loop_info.is_overloaded
            and not self.storage_client_info.is_overloaded
            and not any(info.is_overloaded for info in self.load_signal_info.values())
        )

    def __str__(self) -> str:
        """Name the overloaded resources with the share of time each spent over its threshold, against its limit."""
        # A list, so that a custom signal named like a built-in resource cannot hide it.
        resources = [
            ('cpu', self.cpu_info),
            ('memory', self.memory_info),
            ('event loop', self.event_loop_info),
            ('storage client', self.storage_client_info),
            *self.load_signal_info.items(),
        ]
        overloaded = [
            f'{name} {info.actual_ratio:.0%} of the time (limit {info.limit_ratio:.0%})'
            for name, info in resources
            if info.is_overloaded
        ]
        return f'overloaded: {", ".join(overloaded)}' if overloaded else 'system idle'


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

        Permanently refusing a `consumer` that has nothing in flight stalls its pool with work still pending.

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
