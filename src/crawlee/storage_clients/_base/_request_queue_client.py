from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.configuration import Configuration
    from crawlee.storage_clients.models import (
        AddRequestsResponse,
        ProcessedRequest,
        Request,
        RequestQueueHead,
        RequestQueueMetadata,
    )


@docs_group('Abstract classes')
class RequestQueueClient(ABC):
    """An abstract class for request queue resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    @property
    @abstractmethod
    def metadata(self) -> RequestQueueMetadata:
        """The metadata of the request queue."""

    @classmethod
    @abstractmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> RequestQueueClient:
        """Open a request queue client.

        Args:
            id: ID of the queue to open. If not provided, a new queue will be created with a random ID.
            name: Name of the queue to open. If not provided, the queue will be unnamed.
            configuration: The configuration object.

        Returns:
            A request queue client.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole request queue and remove all its values.

        The backend method for the `RequestQueue.drop` call.
        """

    @abstractmethod
    async def list_head(
        self,
        *,
        lock_time: timedelta | None = None,
        limit: int | None = None,
    ) -> RequestQueueHead:
        """Retrieve requests from the beginning of the queue.

        Fetches the first requests in the queue. If `lock_time` is provided, the requests will be locked
        for the specified duration, preventing them from being processed by other clients until the lock expires.
        This locking functionality may not be supported by all request queue client implementations.

        Args:
            lock_time: Duration for which to lock the retrieved requests, if supported by the client.
                If None, requests will not be locked.
            limit: Maximum number of requests to retrieve.

        Returns:
            A collection of requests from the beginning of the queue, including lock information if applicable.
        """

    @abstractmethod
    async def add_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
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
    async def update_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        """Update a request in the queue.

        Args:
            request: The updated request.
            forefront: Whether to put the updated request in the beginning or the end of the queue.

        Returns:
            The updated request
        """

    @abstractmethod
    async def is_finished(self) -> bool:
        """Check if the request queue is finished.

        Finished means that all requests in the queue have been processed (the queue is empty) and there
        are no more tasks that could add additional requests to the queue.

        Returns:
            True if the request queue is finished, False otherwise.
        """
