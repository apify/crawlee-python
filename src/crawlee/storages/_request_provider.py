from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING

from crawlee.models import BaseRequestData, Request

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.models import ProcessedRequest


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
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        """Reclaims a failed request back to the queue, so that it can be returned for processing later again.

        It is possible to modify the request data by supplying an updated request as a parameter.
        """

    @abstractmethod
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Marks a request as handled after a successful processing (or after giving up retrying)."""

    @abstractmethod
    async def get_handled_count(self) -> int:
        """Returns the number of handled requests."""

    @abstractmethod
    async def add_requests_batched(
        self,
        requests: Sequence[str | BaseRequestData | Request],
        *,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        """Add requests to the underlying resource client in batches.

        Args:
            requests: Requests to add to the queue.
            batch_size: The number of requests to add in one batch.
            wait_time_between_batches: Time to wait between adding batches.
            wait_for_all_requests_to_be_added: If True, wait for all requests to be added before returning.
            wait_for_all_requests_to_be_added_timeout: Timeout for waiting for all requests to be added.
        """

    def _transform_request(self, request: str | BaseRequestData | Request) -> Request:
        """Transforms a request-like object into a Request object."""
        if isinstance(request, Request):
            return request

        if isinstance(request, str):
            return Request.from_url(request)

        if isinstance(request, BaseRequestData):
            return Request.from_base_request_data(request)

        raise ValueError(f'Invalid request type: {type(request)}')

    def _transform_requests(self, requests: Sequence[str | BaseRequestData | Request]) -> list[Request]:
        """Transforms a list of request-like objects into a list of Request objects."""
        processed_requests: list[Request] = []

        for request in requests:
            processed_request = self._transform_request(request)
            processed_requests.append(processed_request)

        return processed_requests
