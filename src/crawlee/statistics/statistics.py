# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/core/src/crawlers/statistics.ts
from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import Self

from crawlee.events import EventManager
from crawlee.statistics.types import FinalStatistics

if TYPE_CHECKING:
    from types import TracebackType


class Statistics:
    __next_id = 0

    def __init__(
        self,
        *,
        event_manager: EventManager | None = None,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str | None = None,
    ) -> None:
        self._id = 0
        self.__next_id += 1

        if persist_state_key is None:
            persist_state_key = f'SDK_CRAWLER_STATISTICS_{self._id}'

    async def __aenter__(self) -> Self:
        """Subscribe to events and start collecting statistics."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop collecting statistics."""

    def register_status_code(self, code: int) -> None:
        pass

    def start_job(self, job_id: str) -> None:
        pass

    def finish_job(self, job_id: str) -> None:
        pass

    def fail_job(self, job_id: str) -> None:
        pass

    def calculate(self) -> FinalStatistics:
        pass
