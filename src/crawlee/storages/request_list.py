from __future__ import annotations

from collections import deque
from dataclasses import asdict
from typing import TYPE_CHECKING

from crawlee._utils.unique_key import compute_unique_key
from crawlee.basic_crawler.types import RequestData
from crawlee.storages.request_provider import RequestProvider

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.basic_crawler.types import CreateRequestSchema


class RequestList(RequestProvider):
    def __init__(self, sources: list[str] | None = None, name: str | None = None) -> None:
        self._name = name or ''
        self._handled_count = 0
        self._sources = deque(
            RequestData(id=compute_unique_key(url), unique_key=compute_unique_key(url), url=url)
            for url in sources or []
        )

    @property
    def name(self) -> str:
        return self._name

    async def get_total_count(self) -> int:
        return len(self._sources)

    async def is_empty(self) -> bool:
        return len(self._sources) == 0

    async def is_finished(self) -> bool:
        return await self.is_empty()

    async def drop(self) -> None:
        self._sources.clear()

    async def fetch_next_request(self) -> RequestData | None:
        try:
            return self._sources.popleft()
        except IndexError:
            return None

    async def reclaim_request(self, request: RequestData, *, forefront: bool = False) -> None:
        if forefront:
            self._sources.appendleft(request)
        else:
            self._sources.append(request)

    async def mark_request_handled(self, request: RequestData) -> None:
        self._handled_count += 1

    async def get_handled_count(self) -> int:
        return self._handled_count

    async def add_requests_batched(
        self,
        requests: list[CreateRequestSchema],
        *,
        batch_size: int,
        wait_for_all_requests_to_be_added: bool,
        wait_time_between_batches: timedelta,
    ) -> None:
        self._sources.extend(RequestData(id=request.unique_key, **asdict(request)) for request in requests)
