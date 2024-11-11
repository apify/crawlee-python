from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING

from crawlee._request import Request
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.base_storage_client._models import ProcessedRequest


@docs_group('Abstract classes')
class RequestProvider(ABC):
    """Abstract base class defining the interface and common behaviour for request providers.

    Request providers are used to manage and provide access to a storage of crawling requests.

    Key responsibilities:
        - Fetching the next request to be processed.
        - Reclaiming requests that failed during processing, allowing retries.
        - Marking requests as successfully handled after processing.
        - Adding new requests to the provider, both individually and in batches.
        - Managing state information such as the total and handled request counts.
        - Deleting or dropping the provider from the underlying storage.

    Subclasses of `RequestProvider` should provide specific implementations for each of the abstract methods.
    """

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
    async def add_request(
        self,
        request: str | Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        """Add a single request to the provider and store it in underlying resource client.

        Args:
            request: The request object (or its string representation) to be added to the provider.
            forefront: Determines whether the request should be added to the beginning (if True) or the end (if False)
                of the provider.

        Returns:
            Information about the request addition to the provider.
        """

    async def add_requests_batched(
        self,
        requests: Sequence[str | Request],
        *,
        batch_size: int = 1000,  # noqa: ARG002
        wait_time_between_batches: timedelta = timedelta(seconds=1),  # noqa: ARG002
        wait_for_all_requests_to_be_added: bool = False,  # noqa: ARG002
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,  # noqa: ARG002
    ) -> None:
        """Add requests to the underlying resource client in batches.

        Args:
            requests: Requests to add to the queue.
            batch_size: The number of requests to add in one batch.
            wait_time_between_batches: Time to wait between adding batches.
            wait_for_all_requests_to_be_added: If True, wait for all requests to be added before returning.
            wait_for_all_requests_to_be_added_timeout: Timeout for waiting for all requests to be added.
        """
        # Default and dumb implementation.
        for request in requests:
            await self.add_request(request)

    def _transform_request(self, request: str | Request) -> Request:
        """Transforms a request-like object into a Request object."""
        if isinstance(request, Request):
            return request

        if isinstance(request, str):
            return Request.from_url(request)

        raise ValueError(f'Invalid request type: {type(request)}')

    def _transform_requests(self, requests: Sequence[str | Request]) -> list[Request]:
        """Transforms a list of request-like objects into a list of Request objects."""
        processed_requests = dict[str, Request]()

        for request in requests:
            processed_request = self._transform_request(request)
            processed_requests.setdefault(processed_request.unique_key, processed_request)

        return list(processed_requests.values())
