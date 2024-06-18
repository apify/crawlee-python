from __future__ import annotations

from collections import deque
from datetime import timedelta
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.storages.request_provider import RequestProvider

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.models import BaseRequestData, Request


class RequestList(RequestProvider):
    """Represents a (potentially very large) list of URLs to crawl."""

    def __init__(
        self,
        requests: Sequence[str | BaseRequestData | Request] | None = None,
        name: str | None = None,
    ) -> None:
        """Initialize the RequestList.

        Args:
            requests: the URLs (or crawling requests) to crawl
            name: a name of the request list
        """
        self._name = name or ''
        self._handled_count = 0

        self._requests = deque(self._transform_requests(requests or []))
        self._in_progress = set[str]()

    @property
    @override
    def name(self) -> str:
        return self._name

    @override
    async def get_total_count(self) -> int:
        return len(self._requests)

    @override
    async def is_empty(self) -> bool:
        return len(self._requests) == 0

    @override
    async def is_finished(self) -> bool:
        return await self.is_empty() and len(self._in_progress) == 0

    @override
    async def drop(self) -> None:
        self._requests.clear()

    @override
    async def fetch_next_request(self) -> Request | None:
        try:
            request = self._requests.popleft()
        except IndexError:
            return None
        else:
            self._in_progress.add(request.id)
            return request

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> None:
        if forefront:
            self._requests.appendleft(request)
        else:
            self._requests.append(request)

        self._in_progress.remove(request.id)

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        self._handled_count += 1
        self._in_progress.remove(request.id)

    @override
    async def get_handled_count(self) -> int:
        return self._handled_count

    @override
    async def add_requests_batched(
        self,
        requests: Sequence[str | BaseRequestData | Request],
        *,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        transformed_requests = self._transform_requests(requests)
        self._requests.extend(transformed_requests)
