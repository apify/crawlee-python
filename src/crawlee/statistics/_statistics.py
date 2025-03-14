# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/core/src/crawlers/statistics.ts
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Generic, cast

from typing_extensions import Self, TypeVar

from crawlee import service_locator
from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask
from crawlee.events._types import Event, EventPersistStateData
from crawlee.statistics import FinalStatistics, StatisticsPersistedState, StatisticsState
from crawlee.statistics._error_tracker import ErrorTracker
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from types import TracebackType

TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)
TNewStatisticsState = TypeVar('TNewStatisticsState', bound=StatisticsState, default=StatisticsState)
logger = getLogger(__name__)


class RequestProcessingRecord:
    """Tracks information about the processing of a request."""

    def __init__(self) -> None:
        self._last_run_at: datetime | None = None
        self._runs = 0
        self.duration: timedelta | None = None

    def run(self) -> int:
        """Mark the job as started."""
        self._last_run_at = datetime.now(timezone.utc)
        self._runs += 1
        return self._runs

    def finish(self) -> timedelta:
        """Mark the job as finished."""
        if self._last_run_at is None:
            raise RuntimeError('Invalid state')

        self.duration = datetime.now(timezone.utc) - self._last_run_at
        return self.duration

    @property
    def retry_count(self) -> int:
        """Number of times the job has been retried."""
        return max(0, self._runs - 1)


@docs_group('Classes')
class Statistics(Generic[TStatisticsState]):
    """A class for collecting, tracking, and logging runtime statistics for requests.

    It is designed to record information such as request durations, retries, successes, and failures, enabling
    analysis of crawler performance. The collected statistics are persisted to a `KeyValueStore`, ensuring they
    remain available across crawler migrations, abortions, and restarts. This persistence allows for tracking
    and evaluation of crawler behavior over its lifecycle.
    """

    __next_id = 0

    def __init__(
        self,
        *,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str | None = None,
        persist_state_key: str | None = None,
        key_value_store: KeyValueStore | None = None,
        log_message: str = 'Statistics',
        periodic_message_logger: Logger | None = None,
        log_interval: timedelta = timedelta(minutes=1),
        state_model: type[TStatisticsState],
    ) -> None:
        self._id = Statistics.__next_id
        Statistics.__next_id += 1

        self._state_model = state_model
        self.state = self._state_model()
        self._instance_start: datetime | None = None
        self._retry_histogram = dict[int, int]()

        self.error_tracker = ErrorTracker()
        self.error_tracker_retry = ErrorTracker()

        self._requests_in_progress = dict[str, RequestProcessingRecord]()

        if persist_state_key is None:
            persist_state_key = f'SDK_CRAWLER_STATISTICS_{self._id}'

        self._persistence_enabled = persistence_enabled
        self._persist_state_key = persist_state_key
        self._persist_state_kvs_name = persist_state_kvs_name
        self._key_value_store: KeyValueStore | None = key_value_store

        self._log_message = log_message
        self._periodic_message_logger = periodic_message_logger or logger
        self._periodic_logger = RecurringTask(self._log, log_interval)

        # Flag to indicate the context state.
        self._active = False

    def replace_state_model(self, state_model: type[TNewStatisticsState]) -> Statistics[TNewStatisticsState]:
        """Create near copy of the `Statistics` with replaced `state_model`."""
        new_statistics: Statistics[TNewStatisticsState] = Statistics(
            persistence_enabled=self._persistence_enabled,
            persist_state_kvs_name=self._persist_state_kvs_name,
            persist_state_key=self._persist_state_key,
            key_value_store=self._key_value_store,
            log_message=self._log_message,
            periodic_message_logger=self._periodic_message_logger,
            state_model=state_model,
        )
        new_statistics._periodic_logger = self._periodic_logger  # Accessing private member to create copy like-object.
        return new_statistics

    @staticmethod
    def with_default_state(
        *,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str | None = None,
        persist_state_key: str | None = None,
        key_value_store: KeyValueStore | None = None,
        log_message: str = 'Statistics',
        periodic_message_logger: Logger | None = None,
        log_interval: timedelta = timedelta(minutes=1),
    ) -> Statistics[StatisticsState]:
        """Convenience constructor for creating a `Statistics` with default state model `StatisticsState`."""
        return Statistics[StatisticsState](
            persistence_enabled=persistence_enabled,
            persist_state_kvs_name=persist_state_kvs_name,
            persist_state_key=persist_state_key,
            key_value_store=key_value_store,
            log_message=log_message,
            periodic_message_logger=periodic_message_logger,
            log_interval=log_interval,
            state_model=StatisticsState,
        )

    @property
    def active(self) -> bool:
        """Indicate whether the context is active."""
        return self._active

    async def __aenter__(self) -> Self:
        """Subscribe to events and start collecting statistics.

        Raises:
            RuntimeError: If the context manager is already active.
        """
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True
        self._instance_start = datetime.now(timezone.utc)

        if self.state.crawler_started_at is None:
            self.state.crawler_started_at = datetime.now(timezone.utc)

        if self._key_value_store is None:
            self._key_value_store = await KeyValueStore.open(name=self._persist_state_kvs_name)

        await self._maybe_load_statistics()
        event_manager = service_locator.get_event_manager()
        event_manager.on(event=Event.PERSIST_STATE, listener=self._persist_state)
        self._periodic_logger.start()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop collecting statistics.

        Raises:
            RuntimeError: If the context manager is not active.
        """
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        self.state.crawler_finished_at = datetime.now(timezone.utc)
        event_manager = service_locator.get_event_manager()
        event_manager.off(event=Event.PERSIST_STATE, listener=self._persist_state)
        await self._periodic_logger.stop()
        await self._persist_state(event_data=EventPersistStateData(is_migrating=False))
        self._active = False

    @ensure_context
    def register_status_code(self, code: int) -> None:
        """Increment the number of times a status code has been received."""
        self.state.requests_with_status_code.setdefault(str(code), 0)
        self.state.requests_with_status_code[str(code)] += 1

    @ensure_context
    def record_request_processing_start(self, request_id_or_key: str) -> None:
        """Mark a request as started."""
        record = self._requests_in_progress.get(request_id_or_key, RequestProcessingRecord())
        record.run()
        self._requests_in_progress[request_id_or_key] = record

    @ensure_context
    def record_request_processing_finish(self, request_id_or_key: str) -> None:
        """Mark a request as finished."""
        record = self._requests_in_progress.get(request_id_or_key)
        if record is None:
            return

        duration = record.finish()
        self.state.requests_finished += 1
        self.state.request_total_finished_duration += duration
        self._save_retry_count_for_request(record)
        self.state.request_min_duration = min(
            self.state.request_min_duration if self.state.request_min_duration is not None else timedelta.max, duration
        )
        self.state.request_max_duration = min(
            self.state.request_max_duration if self.state.request_max_duration is not None else timedelta(), duration
        )

        del self._requests_in_progress[request_id_or_key]

    @ensure_context
    def record_request_processing_failure(self, request_id_or_key: str) -> None:
        """Mark a request as failed."""
        record = self._requests_in_progress.get(request_id_or_key)
        if record is None:
            return

        self.state.request_total_failed_duration += record.finish()
        self.state.requests_failed += 1
        self._save_retry_count_for_request(record)

        del self._requests_in_progress[request_id_or_key]

    def calculate(self) -> FinalStatistics:
        """Calculate the current statistics."""
        if self._instance_start is None:
            raise RuntimeError('The Statistics object is not initialized')

        crawler_runtime = datetime.now(timezone.utc) - self._instance_start
        total_minutes = crawler_runtime.total_seconds() / 60

        return FinalStatistics(
            request_avg_failed_duration=(self.state.request_total_failed_duration / self.state.requests_failed)
            if self.state.requests_failed
            else None,
            request_avg_finished_duration=(self.state.request_total_finished_duration / self.state.requests_finished)
            if self.state.requests_finished
            else None,
            requests_finished_per_minute=round(self.state.requests_finished / total_minutes) if total_minutes else 0,
            requests_failed_per_minute=math.floor(self.state.requests_failed / total_minutes) if total_minutes else 0,
            request_total_duration=self.state.request_total_finished_duration
            + self.state.request_total_failed_duration,
            requests_total=self.state.requests_failed + self.state.requests_finished,
            crawler_runtime=crawler_runtime,
            requests_finished=self.state.requests_finished,
            requests_failed=self.state.requests_failed,
            retry_histogram=[
                self._retry_histogram.get(retry_count, 0)
                for retry_count in range(max(self._retry_histogram.keys(), default=0) + 1)
            ],
        )

    async def reset(self) -> None:
        """Reset the statistics to their defaults and remove any persistent state."""
        self.state = self._state_model()
        self.error_tracker = ErrorTracker()
        self.error_tracker_retry = ErrorTracker()
        self._retry_histogram.clear()
        self._requests_in_progress.clear()

        if self._persistence_enabled and self._key_value_store:
            await self._key_value_store.set_value(self._persist_state_key, None)

    def _log(self) -> None:
        stats = self.calculate()
        self._periodic_message_logger.info(f'{self._log_message}\n{stats.to_table()}')

    async def _maybe_load_statistics(self) -> None:
        if not self._persistence_enabled:
            return

        if not self._key_value_store:
            return

        stored_state = await self._key_value_store.get_value(self._persist_state_key, cast('Any', {}))

        saved_state = self.state.__class__.model_validate(stored_state)
        self.state = saved_state

        if saved_state.stats_persisted_at is not None and saved_state.crawler_last_started_at:
            self._instance_start = datetime.now(timezone.utc) - (
                saved_state.stats_persisted_at - saved_state.crawler_last_started_at
            )
        elif saved_state.crawler_last_started_at:
            self._instance_start = saved_state.crawler_last_started_at

    async def _persist_state(self, event_data: EventPersistStateData) -> None:
        logger.debug(f'Persisting state of the Statistics (event_data={event_data}).')

        if not self._persistence_enabled:
            return

        if not self._key_value_store:
            return

        if not self._instance_start:
            return

        final_statistics = self.calculate()
        persisted_state = StatisticsPersistedState(
            stats_id=self._id,
            stats_persisted_at=datetime.now(timezone.utc),
            crawler_last_started_at=self._instance_start,
            request_total_duration=final_statistics.request_total_duration,
            request_avg_failed_duration=final_statistics.request_avg_failed_duration,
            request_avg_finished_duration=final_statistics.request_avg_finished_duration,
            requests_total=final_statistics.requests_total,
            request_retry_histogram=final_statistics.retry_histogram,
        )

        logger.debug('Persisting state')

        await self._key_value_store.set_value(
            self._persist_state_key,
            self.state.model_dump(mode='json', by_alias=True) | persisted_state.model_dump(mode='json', by_alias=True),
            'application/json',
        )

    def _save_retry_count_for_request(self, record: RequestProcessingRecord) -> None:
        retry_count = record.retry_count

        if retry_count:
            self.state.requests_retries += 1

        self._retry_histogram.setdefault(retry_count, 0)
        self._retry_histogram[retry_count] += 1
