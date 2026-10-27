from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from crawlee import service_locator
from crawlee._utils.docs import docs_group

from ._cpu_load_signal import CpuLoadSignal, CpuLoadSignalOptions
from ._event_loop_load_signal import EventLoopLoadSignal, EventLoopLoadSignalOptions
from ._load_signal import LoadSignalStartContext
from ._memory_load_signal import MemoryLoadSignal, MemoryLoadSignalOptions
from ._storage_client_load_signal import StorageClientLoadSignal, StorageClientLoadSignalOptions

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import timedelta
    from types import TracebackType
    from typing import Self

    from crawlee.configuration import Configuration

    from ._load_signal import LoadSignal


@dataclass(frozen=True, kw_only=True)
@docs_group('Autoscaling')
class LoadSignalsOptions:
    """The load signals a `ConcurrencySystem` watches to decide whether the machine is overloaded.

    Each built-in signal takes the options of its class, and `False` switches it off. How far back the signals are
    evaluated is set by `snapshot_history` and `current_history` of the `ConcurrencySystem`.
    """

    memory: MemoryLoadSignalOptions | Literal[False] = field(default_factory=MemoryLoadSignalOptions)
    """Options of the built-in `MemoryLoadSignal`, or `False` to switch it off."""

    event_loop: EventLoopLoadSignalOptions | Literal[False] = field(default_factory=EventLoopLoadSignalOptions)
    """Options of the built-in `EventLoopLoadSignal`, or `False` to switch it off."""

    cpu: CpuLoadSignalOptions | Literal[False] = field(default_factory=CpuLoadSignalOptions)
    """Options of the built-in `CpuLoadSignal`, or `False` to switch it off."""

    storage_client: StorageClientLoadSignalOptions | Literal[False] = field(
        default_factory=StorageClientLoadSignalOptions
    )
    """Options of the built-in `StorageClientLoadSignal`, or `False` to switch it off."""

    custom: Sequence[LoadSignal] = ()
    """Additional load signals, evaluated alongside the built-in ones. Their names must not collide with the name of
    an enabled built-in signal."""


@docs_group('Autoscaling')
class Snapshotter:
    """Owns the load signals of a `ConcurrencySystem`, built-in and custom, and drives their lifecycle.

    It is utilized by the `ConcurrencySystem`, which configures it through its `load_signals` option.
    """

    def __init__(
        self,
        load_signals: LoadSignalsOptions,
        *,
        max_sample_window: timedelta,
        configuration: Configuration | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            load_signals: The built-in signals to build, and the custom signals to drive alongside them.
            max_sample_window: The longest window the signals are sampled over, and so how much history they keep.
            configuration: The `Configuration` handed to the signals when they start. Uses the global one if not
                provided.
        """
        self._max_sample_window = max_sample_window
        self._configuration = configuration
        self._load_signals: list[LoadSignal] = []

        if load_signals.memory is not False:
            self._load_signals.append(MemoryLoadSignal(load_signals.memory))

        if load_signals.event_loop is not False:
            self._load_signals.append(EventLoopLoadSignal(load_signals.event_loop))

        if load_signals.cpu is not False:
            self._load_signals.append(CpuLoadSignal(load_signals.cpu))

        if load_signals.storage_client is not False:
            self._load_signals.append(StorageClientLoadSignal(load_signals.storage_client))

        self._load_signals.extend(load_signals.custom)

    @property
    def load_signals(self) -> list[LoadSignal]:
        """The built-in signals that were not switched off, followed by the custom ones."""
        return list(self._load_signals)

    async def __aenter__(self) -> Self:
        """Start all signals. A signal that fails to start is stopped along with the ones started before it."""
        context = LoadSignalStartContext(
            max_sample_window=self._max_sample_window,
            configuration=self._configuration or service_locator.get_configuration(),
        )

        async with AsyncExitStack() as started:
            for signal in self._load_signals:
                started.push_async_callback(signal.stop)
                await signal.start(context)

            started.pop_all()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop all signals, even if stopping one of them fails."""
        async with AsyncExitStack() as stopping:
            for signal in self._load_signals:
                stopping.push_async_callback(signal.stop)
