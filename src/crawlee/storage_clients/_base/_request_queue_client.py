from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata


@docs_group('Abstract classes')
class RequestQueueClient(ABC):
    """An abstract class for request queue resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    @abstractmethod
    async def get_metadata(self) -> RequestQueueMetadata:
        """Get the metadata of the request queue."""

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole request queue and remove all its values.

        The backend method for the `RequestQueue.drop` call.
        """

    @abstractmethod
    async def purge(self) -> None:
        """Purge all items from the request queue.

        The backend method for the `RequestQueue.purge` call.
        """

    @abstractmethod
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        """Add batch of requests to the queue.

        This method adds a batch of requests to the queue. Each request is processed based on its uniqueness
        (determined by `unique_key`). Duplicates will be identified but not re-added to the queue.

        Args:
            requests: The collection of requests to add to the queue.
            forefront: Whether to put the added requests at the beginning (True) or the end (False) of the queue.
                When True, the requests will be processed sooner than previously added requests.
            batch_size: The maximum number of requests to add in a single batch.
            wait_time_between_batches: The time to wait between adding batches of requests.
            wait_for_all_requests_to_be_added: If True, the method will wait until all requests are added
                to the queue before returning.
            wait_for_all_requests_to_be_added_timeout: The maximum time to wait for all requests to be added.

        Returns:
            A response object containing information about which requests were successfully
            processed and which failed (if any).
        """

    @abstractmethod
    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve.

        Returns:
            The retrieved request, or None, if it did not exist.
        """

    @abstractmethod
    async def fetch_next_request(self) -> Request | None:
        """Return the next request in the queue to be processed.

        Once you successfully finish processing of the request, you need to call `RequestQueue.mark_request_as_handled`
        to mark the request as handled in the queue. If there was some error in processing the request, call
        `RequestQueue.reclaim_request` instead, so that the queue will give the request to some other consumer
        in another call to the `fetch_next_request` method.

        Note that the `None` return value does not mean the queue processing finished, it means there are currently
        no pending requests. To check whether all requests in queue were finished, use `RequestQueue.is_finished`
        instead.

        Returns:
            The request or `None` if there are no more pending requests.
        """

    @abstractmethod
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `RequestQueue.fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """

    @abstractmethod
    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        """Reclaim a failed request back to the queue.

        The request will be returned for processing later again by another call to `RequestQueue.fetch_next_request`.

        Args:
            request: The request to return to the queue.
            forefront: Whether to add the request to the head or the end of the queue.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """

    @abstractmethod
    async def is_empty(self) -> bool:
        """Check if the request queue is empty.

        Returns:
            True if the request queue is empty, False otherwise.
        """
