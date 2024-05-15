# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/core/src/crawlers/statistics.ts
from __future__ import annotations

import math
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generic, cast

from typing_extensions import Self, TypeVar

from crawlee._utils.recurring_task import RecurringTask
from crawlee.events.local_event_manager import LocalEventManager
from crawlee.events.types import Event
from crawlee.statistics.error_tracker import ErrorTracker
from crawlee.statistics.models import FinalStatistics, StatisticsPersistedState, StatisticsState
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager

TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)

logger = getLogger(__name__)


class Job:
    """Tracks information about a running job."""

    def __init__(self) -> None:
        self.last_run_at: datetime | None = None
        self.runs = 0
        self.duration: timedelta | None = None

    def run(self) -> int:
        """Mark the job as started."""
        self.last_run_at = datetime.now(timezone.utc)
        self.runs += 1
        return self.runs

    def finish(self) -> timedelta:
        """Mark the job as finished."""
        if self.last_run_at is None:
            raise RuntimeError('Invalid state')

        self.duration = datetime.now(timezone.utc) - self.last_run_at
        return self.duration

    @property
    def retry_count(self) -> int:
        """Number of times the job has been retried."""
        return max(0, self.runs - 1)


class Statistics(Generic[TStatisticsState]):
    """An interface to collecting and logging runtime statistics for requests.

    All information is saved to the key value store so that it persists between migrations, abortions and resurrections.
    """

    __next_id = 0

    def __init__(
        self,
        *,
        event_manager: EventManager | None = None,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str | None = None,
        key_value_store: KeyValueStore | None = None,
        log_message: str = 'Statistics',
        log_interval: timedelta = timedelta(minutes=1),
        state_model: type[TStatisticsState] = cast(Any, StatisticsState),  # noqa: B008 - in an ideal world, TStatisticsState would be inferred from this argument, but I haven't managed to do that
    ) -> None:
        self._id = self.__next_id
        self.__next_id += 1

        self._state_model = state_model
        self.state: StatisticsState = self._state_model()
        self._instance_start: datetime | None = None
        self._retry_histogram = dict[int, int]()

        self.error_tracker = ErrorTracker()
        self.error_tracker_retry = ErrorTracker()

        self._events = event_manager or LocalEventManager()

        self._requests_in_progress = dict[str, Job]()

        if persist_state_key is None:
            persist_state_key = f'SDK_CRAWLER_STATISTICS_{self._id}'

        self._persistence_enabled = persistence_enabled
        self._persist_state_key = persist_state_key
        self._persist_state_kvs_name = persist_state_kvs_name
        self._key_value_store: KeyValueStore | None = key_value_store

        self._log_message = log_message
        self._periodic_logger = RecurringTask(self._log, log_interval)

    async def __aenter__(self) -> Self:
        """Subscribe to events and start collecting statistics."""
        self._instance_start = datetime.now(timezone.utc)

        if self.state.crawler_started_at is None:
            self.state.crawler_started_at = datetime.now(timezone.utc)

        if self._key_value_store is None:
            self._key_value_store = await KeyValueStore.open(name=self._persist_state_kvs_name)

        await self._maybe_load_statistics()
        self._events.on(event=Event.PERSIST_STATE, listener=self._persist_state)

        self._periodic_logger.start()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop collecting statistics."""
        self.state.crawler_finished_at = datetime.now(timezone.utc)
        self._events.off(event=Event.PERSIST_STATE, listener=self._persist_state)
        await self._periodic_logger.stop()
        await self._persist_state()

    def register_status_code(self, code: int) -> None:
        """Increment the number of times a status code has been received."""
        self.state.requests_with_status_code.setdefault(str(code), 0)
        self.state.requests_with_status_code[str(code)] += 1

    def start_job(self, job_id: str) -> None:
        """Mark a job as started."""
        job = self._requests_in_progress.get(job_id, Job())
        job.run()
        self._requests_in_progress[job_id] = job

    def finish_job(self, job_id: str) -> None:
        """Mark a job as finished."""
        job = self._requests_in_progress.get(job_id)
        if job is None:
            return

        duration = job.finish()
        self.state.requests_finished += 1
        self.state.request_total_finished_duration += duration
        self._save_retry_count_for_job(job)
        self.state.request_min_duration = min(self.state.request_min_duration, duration)

        del self._requests_in_progress[job_id]

    def fail_job(self, job_id: str) -> None:
        """Mark a job as failed."""
        job = self._requests_in_progress.get(job_id)
        if job is None:
            return

        self.state.request_total_finished_duration += job.finish()
        self.state.requests_failed += 1
        self._save_retry_count_for_job(job)

        del self._requests_in_progress[job_id]

    def calculate(self) -> FinalStatistics:
        """Calculate the current statistics."""
        if self._instance_start is None:
            raise RuntimeError('The Statistics object is not initialized')

        crawler_runtime = datetime.now(timezone.utc) - self._instance_start
        total_minutes = crawler_runtime.total_seconds() / 60

        return FinalStatistics(
            request_avg_failed_duration=(self.state.request_total_failed_duration / self.state.requests_failed)
            if self.state.requests_failed
            else timedelta.max,
            request_avg_finished_duration=(self.state.request_total_finished_duration / self.state.requests_finished)
            if self.state.requests_finished
            else timedelta.max,
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
        logger.info(f'{self._log_message} {asdict(stats)}')

    async def _maybe_load_statistics(self) -> None:
        if not self._persistence_enabled:
            return

        if not self._key_value_store:
            return

        stored_state = await self._key_value_store.get_value(self._persist_state_key, cast(Any, {}))

        saved_state = self.state.__class__.model_validate(stored_state)
        self.state = saved_state

        if saved_state.stats_persisted_at is not None and saved_state.crawler_last_started_at:
            self._instance_start = datetime.now(timezone.utc) - (
                saved_state.stats_persisted_at - saved_state.crawler_last_started_at
            )
        elif saved_state.crawler_last_started_at:
            self._instance_start = saved_state.crawler_last_started_at

    async def _persist_state(self) -> None:
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

    def _save_retry_count_for_job(self, job: Job) -> None:
        retry_count = job.retry_count

        if retry_count:
            self.state.requests_retries += 1

        self._retry_histogram.setdefault(retry_count, 0)
        self._retry_histogram[retry_count] += 1
