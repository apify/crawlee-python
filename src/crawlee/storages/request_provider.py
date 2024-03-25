from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Protocol

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.basic_crawler.types import CreateRequestSchema, RequestData


class RequestProvider(Protocol):
    @property
    def name(self) -> str: ...

    async def get_total_count(self) -> int: ...

    async def is_empty(self) -> bool: ...

    async def is_finished(self) -> bool: ...

    async def drop(self) -> None: ...

    async def fetch_next_request(self) -> RequestData | None: ...

    async def reclaim_request(self, request: RequestData, *, forefront: bool = False) -> None: ...

    async def mark_request_handled(self, request: RequestData) -> None: ...

    async def get_handled_count(self) -> int: ...

    async def add_requests_batched(
        self,
        requests: list[CreateRequestSchema],
        *,
        batch_size: int,
        wait_for_all_requests_to_be_added: bool,
        wait_time_between_batches: timedelta,
    ) -> None: ...
