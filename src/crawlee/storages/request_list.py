from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee._utils.requests import compute_unique_key
from crawlee.basic_crawler.types import RequestData
from crawlee.storages.request_provider import RequestProvider

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.basic_crawler.types import CreateRequestSchema


class RequestList(RequestProvider):
    """Represents a (potentially very large) list of URLs to crawl."""

    def __init__(self, sources: list[str | RequestData] | None = None, name: str | None = None) -> None:
        self._name = name or ''
        self._handled_count = 0
        self._sources = deque(
            url
            if isinstance(url, RequestData)
            else RequestData(id=compute_unique_key(url), unique_key=compute_unique_key(url), url=url)
            for url in sources or []
        )
        self._in_progress = set[str]()

    @property
    @override
    def name(self) -> str:
        return self._name

    @override
    async def get_total_count(self) -> int:
        return len(self._sources)

    @override
    async def is_empty(self) -> bool:
        return len(self._sources) == 0

    @override
    async def is_finished(self) -> bool:
        return await self.is_empty() and len(self._in_progress) == 0

    @override
    async def drop(self) -> None:
        self._sources.clear()

    @override
    async def fetch_next_request(self) -> RequestData | None:
        try:
            request = self._sources.popleft()
        except IndexError:
            return None
        else:
            self._in_progress.add(request.id)
            return request

    @override
    async def reclaim_request(self, request: RequestData, *, forefront: bool = False) -> None:
        if forefront:
            self._sources.appendleft(request)
        else:
            self._sources.append(request)

        self._in_progress.remove(request.id)

    @override
    async def mark_request_handled(self, request: RequestData) -> None:
        self._handled_count += 1
        self._in_progress.remove(request.id)

    @override
    async def get_handled_count(self) -> int:
        return self._handled_count

    @override
    async def add_requests_batched(
        self,
        requests: list[CreateRequestSchema],
        *,
        batch_size: int,
        wait_for_all_requests_to_be_added: bool,
        wait_time_between_batches: timedelta,
    ) -> None:
        self._sources.extend(RequestData(id=request.unique_key, **request.model_dump()) for request in requests)
