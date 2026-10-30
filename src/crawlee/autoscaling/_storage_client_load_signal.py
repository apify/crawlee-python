from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask

from ._load_signal import LoadSnapshot, SnapshotStore

if TYPE_CHECKING:
    from crawlee.storage_clients import StorageClient

    from ._load_signal import LoadSignalStartContext


@dataclass(kw_only=True)
class StorageClientSnapshot(LoadSnapshot):
    """A snapshot taken by the `StorageClientLoadSignal`."""

    rate_limit_error_count: int
    """The number of rate-limit errors (HTTP 429) the storage client has seen so far."""


@dataclass(frozen=True, kw_only=True)
@docs_group('Autoscaling')
class StorageClientLoadSignalOptions:
    """Options of the built-in `StorageClientLoadSignal`."""

    snapshot_interval: timedelta = timedelta(seconds=1)
    """How often the storage client is checked for new rate-limit errors."""

    overloaded_ratio: float = 0.3
    """The share of the sample window the storage client may spend overloaded before the signal reports overload."""

    def __post_init__(self) -> None:
        if self.snapshot_interval <= timedelta(0):
            raise ValueError('snapshot_interval must be positive')


@docs_group('Autoscaling')
class StorageClientLoadSignal:
    """Checks the storage client for rate-limit errors (HTTP 429).

    The storage client counts as overloaded while its rate-limit errors grow by more than
    `Configuration.max_client_errors` per `snapshot_interval`. Only errors on the second retry of a call count, as
    earlier ones may come from a random spike in requests. Switch this signal off if your storage client reports no
    rate-limit errors, since it otherwise polls the client to no purpose. The `ConcurrencySystem` builds this signal
    by default, so construct one only to wrap or adapt it.
    """

    name = 'storage_client_info'
    """The key of the signal in the reported `SystemInfo`."""

    _RATE_LIMIT_ERROR_RETRY_COUNT = 2
    """Number of retries for a client request before considering it a failure due to rate limiting."""

    def __init__(self, options: StorageClientLoadSignalOptions | None = None) -> None:
        """Initialize a new instance.

        Args:
            options: The options of the signal. Uses the defaults if not provided.
        """
        self._options = options or StorageClientLoadSignalOptions()
        self._max_errors = 0
        self._store = SnapshotStore[StorageClientSnapshot]()
        self._snapshot_task = RecurringTask(self._snapshot, self._options.snapshot_interval)
        self._storage_client: StorageClient | None = None

    @property
    def overloaded_ratio(self) -> float:
        """The `overloaded_ratio` from the options of the signal."""
        return self._options.overloaded_ratio

    async def start(self, context: LoadSignalStartContext) -> None:
        """Start recording snapshots, with the limits from `context.configuration`."""
        self._store.use_sample_window(context.max_sample_window)
        # Otherwise the first measurement counts the errors of the previous session as new ones.
        self._store.clear()

        self._max_errors = context.configuration.max_client_errors

        self._storage_client = service_locator.get_storage_client()
        self._snapshot_task.start()

    async def stop(self) -> None:
        """Stop recording snapshots."""
        await self._snapshot_task.stop()
        self._storage_client = None

    def get_sample(self, duration: timedelta | None = None) -> list[StorageClientSnapshot]:
        """Return the snapshots taken within `duration` of the latest one, or all of them if omitted."""
        return self._store.get_sample(duration)

    def _snapshot(self) -> None:
        """Record the rate-limit errors of the storage client."""
        rate_limit_errors = self._storage_client.get_rate_limit_errors() if self._storage_client else {}
        error_count = rate_limit_errors.get(self._RATE_LIMIT_ERROR_RETRY_COUNT, 0)

        snapshots = self._store.get_sample()
        new_error_count = error_count - snapshots[-1].rate_limit_error_count if snapshots else 0

        snapshot = StorageClientSnapshot(
            rate_limit_error_count=error_count,
            is_overloaded=new_error_count > self._max_errors,
            created_at=datetime.now(UTC),
        )
        self._store.push(snapshot)
