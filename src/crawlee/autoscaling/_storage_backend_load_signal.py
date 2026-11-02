from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask

from ._load_signal import LoadSnapshot, SnapshotStore

if TYPE_CHECKING:
    from crawlee.storage_backends import StorageBackend

    from ._load_signal import LoadSignalStartContext


@dataclass(kw_only=True)
class StorageBackendSnapshot(LoadSnapshot):
    """A snapshot taken by the `StorageBackendLoadSignal`."""

    rate_limit_error_count: int
    """The number of rate-limit errors (HTTP 429) the storage backend has seen so far."""


@dataclass(frozen=True, kw_only=True)
@docs_group('Autoscaling')
class StorageBackendLoadSignalOptions:
    """Options of the built-in `StorageBackendLoadSignal`."""

    snapshot_interval: timedelta = timedelta(seconds=1)
    """How often the storage backend is checked for new rate-limit errors."""

    overloaded_ratio: float = 0.3
    """The share of the sample window the storage backend may spend overloaded before the signal reports overload."""

    def __post_init__(self) -> None:
        if self.snapshot_interval <= timedelta(0):
            raise ValueError('snapshot_interval must be positive')


@docs_group('Autoscaling')
class StorageBackendLoadSignal:
    """Checks the storage backend for rate-limit errors (HTTP 429).

    The storage backend counts as overloaded while its rate-limit errors grow by more than
    `Configuration.max_client_errors` per `snapshot_interval`. Only errors on the second retry of a call count, as
    earlier ones may come from a random spike in requests. Switch this signal off if your storage backend reports no
    rate-limit errors, since it otherwise polls the client to no purpose. The `ConcurrencySystem` builds this signal
    by default, so construct one only to wrap or adapt it.
    """

    name = 'storage_backend_info'
    """The key of the signal in the reported `SystemInfo`."""

    _RATE_LIMIT_ERROR_RETRY_COUNT = 2
    """Number of retries for a client request before considering it a failure due to rate limiting."""

    def __init__(self, options: StorageBackendLoadSignalOptions | None = None) -> None:
        """Initialize a new instance.

        Args:
            options: The options of the signal. Uses the defaults if not provided.
        """
        self._options = options or StorageBackendLoadSignalOptions()
        self._max_errors = 0
        self._store = SnapshotStore[StorageBackendSnapshot]()
        self._snapshot_task = RecurringTask(self._snapshot, self._options.snapshot_interval)
        self._storage_backend: StorageBackend | None = None

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

        self._storage_backend = service_locator.get_storage_backend()
        self._snapshot_task.start()

    async def stop(self) -> None:
        """Stop recording snapshots."""
        await self._snapshot_task.stop()
        self._storage_backend = None

    def get_sample(self, duration: timedelta | None = None) -> list[StorageBackendSnapshot]:
        """Return the snapshots taken within `duration` of the latest one, or all of them if omitted."""
        return self._store.get_sample(duration)

    def _snapshot(self) -> None:
        """Record the rate-limit errors of the storage backend."""
        rate_limit_errors = self._storage_backend.get_rate_limit_errors() if self._storage_backend else {}
        error_count = rate_limit_errors.get(self._RATE_LIMIT_ERROR_RETRY_COUNT, 0)

        snapshots = self._store.get_sample()
        new_error_count = error_count - snapshots[-1].rate_limit_error_count if snapshots else 0

        snapshot = StorageBackendSnapshot(
            rate_limit_error_count=error_count,
            is_overloaded=new_error_count > self._max_errors,
            created_at=datetime.now(UTC),
        )
        self._store.push(snapshot)
