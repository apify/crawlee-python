# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/core/src/crawlers/statistics.ts
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Generic

from typing_extensions import Self, TypeVar

from crawlee.events import EventManager
from crawlee.statistics.models import FinalStatistics, StatisticsPersistedState, StatisticsState

if TYPE_CHECKING:
    from types import TracebackType

TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState)


class Statistics(Generic[TStatisticsState]):
    __next_id = 0

    def __init__(
        self,
        *,
        event_manager: EventManager | None = None,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str | None = None,
        state_model: type[TStatisticsState] = StatisticsState,
    ) -> None:
        self._id = self.__next_id
        self.__next_id += 1

        self.state = state_model()
        self._instance_start: datetime | None = None
        self._instance_end: datetime | None = None
        self._retry_histogram = dict[int, int]()

        if persist_state_key is None:
            persist_state_key = f'SDK_CRAWLER_STATISTICS_{self._id}'

    async def __aenter__(self) -> Self:
        """Subscribe to events and start collecting statistics."""
        self._instance_start = datetime.now(timezone.utc)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop collecting statistics."""
        self._instance_end = datetime.now(timezone.utc)

    def register_status_code(self, code: int) -> None:
        pass

    def start_job(self, job_id: str) -> None:
        pass

    def finish_job(self, job_id: str) -> None:
        pass

    def fail_job(self, job_id: str) -> None:
        pass

    def calculate(self) -> FinalStatistics:
        if self._instance_start is None:
            raise RuntimeError('The Statistics object is not initialized')

        crawler_runtime = (self._instance_end or datetime.now(timezone.utc)) - self._instance_start
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
