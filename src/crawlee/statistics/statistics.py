# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/core/src/crawlers/statistics.ts
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generic, cast

from typing_extensions import Self, TypeVar

from crawlee.events.local_event_manager import LocalEventManager
from crawlee.events.types import Event
from crawlee.statistics.models import FinalStatistics, StatisticsPersistedState, StatisticsState
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager

TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)

logger = getLogger(__name__)


class Statistics(Generic[TStatisticsState]):
    __next_id = 0

    def __init__(
        self,
        *,
        event_manager: EventManager | None = None,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str | None = None,
        key_value_store: KeyValueStore | None = None,
        state_model: type[TStatisticsState] = StatisticsState,
    ) -> None:
        self._id = self.__next_id
        self.__next_id += 1

        self.state = state_model()
        self._instance_start: datetime | None = None
        self._instance_end: datetime | None = None
        self._retry_histogram = dict[int, int]()

        self._events = event_manager or LocalEventManager()

        if persist_state_key is None:
            persist_state_key = f'SDK_CRAWLER_STATISTICS_{self._id}'

        self._persistence_enabled = persistence_enabled
        self._persist_state_key = persist_state_key
        self._persist_state_kvs_name = persist_state_kvs_name
        self._key_value_store: KeyValueStore | None = key_value_store

    async def __aenter__(self) -> Self:
        """Subscribe to events and start collecting statistics."""
        self._instance_start = datetime.now(timezone.utc)

        if self._key_value_store is None:
            self._key_value_store = await KeyValueStore.open(name=self._persist_state_kvs_name)

        self._events.on(event=Event.PERSIST_STATE, listener=self._persist_state)
        await self._maybe_load_statistics()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop collecting statistics."""
        self._instance_end = datetime.now(timezone.utc)
        await self._persist_state()

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

    async def _maybe_load_statistics(self) -> None:
        if not self._persistence_enabled:
            return

        if not self._key_value_store:
            return

        saved_state = self.state.__class__.model_validate(
            await self._key_value_store.get_value(self._persist_state_key, cast(Any, {}))
        )

        self.state = saved_state

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
