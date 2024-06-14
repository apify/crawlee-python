from __future__ import annotations

from collections import deque
from datetime import timedelta
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.models import BaseRequestData, BatchRequestsOperationResponse, ProcessedRequest, Request
from crawlee.storages.request_provider import RequestProvider

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence


class RequestList(RequestProvider):
    """Represents a (potentially very large) list of URLs to crawl."""

    def __init__(self, sources: list[str | Request] | None = None, name: str | None = None) -> None:
        """Initialize the RequestList.

        Args:
            sources: the URLs (or crawling requests) to crawl
            name: a name of the request list
        """
        self._name = name or ''
        self._handled_count = 0
        self._sources = deque(url if isinstance(url, Request) else Request.from_url(url) for url in sources or [])
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
    async def fetch_next_request(self) -> Request | None:
        try:
            request = self._sources.popleft()
        except IndexError:
            return None
        else:
            self._in_progress.add(request.id)
            return request

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> None:
        if forefront:
            self._sources.appendleft(request)
        else:
            self._sources.append(request)

        self._in_progress.remove(request.id)

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        self._handled_count += 1
        self._in_progress.remove(request.id)

    @override
    async def get_handled_count(self) -> int:
        return self._handled_count

    @override
    async def add_requests_batched(  # type: ignore  # mypy has problems here
        self,
        requests: Sequence[BaseRequestData | Request | str],
        *,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
    ) -> AsyncGenerator[BatchRequestsOperationResponse, None]:
        transformed_requests = self._transform_requests(requests)
        self._sources.extend(transformed_requests)

        yield BatchRequestsOperationResponse(
            processed_requests=[
                ProcessedRequest(
                    id=request.id,
                    unique_key=request.unique_key,
                    was_already_present=False,
                    was_already_handled=False,
                )
                for request in transformed_requests
            ],
            unprocessed_requests=[],
        )
