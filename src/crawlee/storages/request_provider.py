from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.request import BaseRequestData, Request
    from crawlee.storages.models import RequestQueueOperationInfo


class RequestProvider(ABC):
    """Provides access to a queue of crawling requests."""

    @property
    @abstractmethod
    def name(self) -> str | None:
        """ID or name of the request queue."""

    @abstractmethod
    async def get_total_count(self) -> int:
        """Returns an offline approximation of the total number of requests in the queue (i.e. pending + handled)."""

    @abstractmethod
    async def is_empty(self) -> bool:
        """Returns True if there are no more requests in the queue (there might still be unfinished requests)."""

    @abstractmethod
    async def is_finished(self) -> bool:
        """Returns True if all requests have been handled."""

    @abstractmethod
    async def drop(self) -> None:
        """Removes the queue either from the Apify Cloud storage or from the local database."""

    @abstractmethod
    async def fetch_next_request(self) -> Request | None:
        """Returns a next request in the queue to be processed, or `null` if there are no more pending requests."""

    @abstractmethod
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> RequestQueueOperationInfo | None:
        """Reclaims a failed request back to the queue, so that it can be returned for processing later again.

        It is possible to modify the request data by supplying an updated request as a parameter.
        """

    @abstractmethod
    async def mark_request_as_handled(self, request: Request) -> RequestQueueOperationInfo | None:
        """Marks a request as handled after a successful processing (or after giving up retrying)."""

    @abstractmethod
    async def get_handled_count(self) -> int:
        """Returns the number of handled requests."""

    @abstractmethod
    async def add_requests_batched(
        self,
        requests: list[BaseRequestData | Request],
        *,
        batch_size: int,
        wait_for_all_requests_to_be_added: bool,
        wait_time_between_batches: timedelta,
    ) -> None:
        """Adds requests to the queue in batches.

        By default, it will resolve after the initial batch is added, and continue adding the rest in background.
        You can configure the batch size via `batch_size` option and the sleep time in between the batches
        via `wait_time_between_batches`. If you want to wait for all batches to be added to the queue, you can use
        the `wait_for_all_requests_to_be_added` option.
        """
