# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/core/src/crawlers/statistics.ts
from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Generic, Literal

from typing_extensions import Self, TypeVar

from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee._utils.recoverable_state import RecoverableState
from crawlee._utils.recurring_task import RecurringTask
from crawlee.statistics import FinalStatistics, StatisticsState
from crawlee.statistics._error_tracker import ErrorTracker

if TYPE_CHECKING:
    from types import TracebackType

TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)
TNewStatisticsState = TypeVar('TNewStatisticsState', bound=StatisticsState, default=StatisticsState)
logger = getLogger(__name__)


class RequestProcessingRecord:
    """Tracks information about the processing of a request."""

    def __init__(self) -> None:
        self._last_run_at_ns: int | None = None
        self._runs = 0
        self.duration: timedelta | None = None

    def run(self) -> int:
        """Mark the job as started."""
        self._last_run_at_ns = time.perf_counter_ns()
        self._runs += 1
        return self._runs

    def finish(self) -> timedelta:
        """Mark the job as finished."""
        if self._last_run_at_ns is None:
            raise RuntimeError('Invalid state')

        self.duration = timedelta(microseconds=math.ceil((time.perf_counter_ns() - self._last_run_at_ns) / 1000))
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
        log_message: str = 'Statistics',
        periodic_message_logger: Logger | None = None,
        log_interval: timedelta = timedelta(minutes=1),
        state_model: type[TStatisticsState],
        statistics_log_format: Literal['table', 'inline'] = 'table',
        save_error_snapshots: bool = False,
    ) -> None:
        self._id = Statistics.__next_id
        Statistics.__next_id += 1

        self._instance_start: datetime | None = None

        self.error_tracker = ErrorTracker(
            save_error_snapshots=save_error_snapshots,
            snapshot_kvs_name=persist_state_kvs_name,
        )
        self.error_tracker_retry = ErrorTracker(save_error_snapshots=False)

        self._requests_in_progress = dict[str, RequestProcessingRecord]()

        self._state = RecoverableState(
            default_state=state_model(stats_id=self._id),
            persist_state_key=persist_state_key or f'SDK_CRAWLER_STATISTICS_{self._id}',
            persistence_enabled=persistence_enabled,
            persist_state_kvs_name=persist_state_kvs_name,
            logger=logger,
        )

        self._log_message = log_message
        self._statistics_log_format = statistics_log_format
        self._periodic_message_logger = periodic_message_logger or logger
        self._periodic_logger = RecurringTask(self._log, log_interval)

        # Flag to indicate the context state.
        self._active = False

    def replace_state_model(self, state_model: type[TNewStatisticsState]) -> Statistics[TNewStatisticsState]:
        """Create near copy of the `Statistics` with replaced `state_model`."""
        new_statistics: Statistics[TNewStatisticsState] = Statistics(
            persistence_enabled=self._state._persistence_enabled,  # noqa: SLF001
            persist_state_kvs_name=self._state._persist_state_kvs_name,  # noqa: SLF001
            persist_state_key=self._state._persist_state_key,  # noqa: SLF001
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
        log_message: str = 'Statistics',
        periodic_message_logger: Logger | None = None,
        log_interval: timedelta = timedelta(minutes=1),
        statistics_log_format: Literal['table', 'inline'] = 'table',
        save_error_snapshots: bool = False,
    ) -> Statistics[StatisticsState]:
        """Initialize a new instance with default state model `StatisticsState`."""
        return Statistics[StatisticsState](
            persistence_enabled=persistence_enabled,
            persist_state_kvs_name=persist_state_kvs_name,
            persist_state_key=persist_state_key,
            log_message=log_message,
            periodic_message_logger=periodic_message_logger,
            log_interval=log_interval,
            state_model=StatisticsState,
            statistics_log_format=statistics_log_format,
            save_error_snapshots=save_error_snapshots,
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

        await self._state.initialize()
        self._after_initialize()

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

        self._state.current_value.crawler_finished_at = datetime.now(timezone.utc)

        await self._state.teardown()

        await self._periodic_logger.stop()

        self._active = False

    @property
    def state(self) -> TStatisticsState:
        return self._state.current_value

    @ensure_context
    def register_status_code(self, code: int) -> None:
        """Increment the number of times a status code has been received."""
        state = self._state.current_value
        state.requests_with_status_code.setdefault(str(code), 0)
        state.requests_with_status_code[str(code)] += 1

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

        state = self._state.current_value
        duration = record.finish()

        state.requests_finished += 1
        state.request_total_finished_duration += duration
        self._save_retry_count_for_request(record)
        state.request_min_duration = min(
            state.request_min_duration if state.request_min_duration is not None else timedelta.max, duration
        )
        state.request_max_duration = min(
            state.request_max_duration if state.request_max_duration is not None else timedelta(), duration
        )

        del self._requests_in_progress[request_id_or_key]

    @ensure_context
    def record_request_processing_failure(self, request_id_or_key: str) -> None:
        """Mark a request as failed."""
        record = self._requests_in_progress.get(request_id_or_key)
        if record is None:
            return

        state = self._state.current_value

        state.request_total_failed_duration += record.finish()
        state.requests_failed += 1
        self._save_retry_count_for_request(record)

        del self._requests_in_progress[request_id_or_key]

    def calculate(self) -> FinalStatistics:
        """Calculate the current statistics."""
        if self._instance_start is None:
            raise RuntimeError('The Statistics object is not initialized')

        crawler_runtime = datetime.now(timezone.utc) - self._instance_start
        total_minutes = crawler_runtime.total_seconds() / 60
        state = self._state.current_value
        serialized_state = state.model_dump(by_alias=False)

        return FinalStatistics(
            request_avg_failed_duration=state.request_avg_failed_duration,
            request_avg_finished_duration=state.request_avg_finished_duration,
            requests_finished_per_minute=round(state.requests_finished / total_minutes) if total_minutes else 0,
            requests_failed_per_minute=math.floor(state.requests_failed / total_minutes) if total_minutes else 0,
            request_total_duration=state.request_total_finished_duration + state.request_total_failed_duration,
            requests_total=state.requests_failed + state.requests_finished,
            crawler_runtime=crawler_runtime,
            requests_finished=state.requests_finished,
            requests_failed=state.requests_failed,
            retry_histogram=serialized_state['request_retry_histogram'],
        )

    async def reset(self) -> None:
        """Reset the statistics to their defaults and remove any persistent state."""
        await self._state.reset()
        self.error_tracker = ErrorTracker()
        self.error_tracker_retry = ErrorTracker()
        self._requests_in_progress.clear()

    def _log(self) -> None:
        stats = self.calculate()
        if self._statistics_log_format == 'table':
            self._periodic_message_logger.info(f'{self._log_message}\n{stats.to_table()}')
        else:
            self._periodic_message_logger.info(self._log_message, extra=stats.to_dict())

    def _after_initialize(self) -> None:
        state = self._state.current_value

        if state.crawler_started_at is None:
            state.crawler_started_at = datetime.now(timezone.utc)

        if state.stats_persisted_at is not None and state.crawler_last_started_at:
            self._instance_start = datetime.now(timezone.utc) - (
                state.stats_persisted_at - state.crawler_last_started_at
            )
        elif state.crawler_last_started_at:
            self._instance_start = state.crawler_last_started_at

        state.crawler_last_started_at = self._instance_start

    def _save_retry_count_for_request(self, record: RequestProcessingRecord) -> None:
        retry_count = record.retry_count
        state = self._state.current_value

        if retry_count:
            state.requests_retries += 1

        state.request_retry_histogram.setdefault(retry_count, 0)
        state.request_retry_histogram[retry_count] += 1
