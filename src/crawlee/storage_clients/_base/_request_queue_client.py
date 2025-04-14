from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from crawlee.storage_clients.models import (
        BatchRequestsOperationResponse,
        ProcessedRequest,
        ProlongRequestLockResponse,
        Request,
        RequestQueueHeadWithLocks,
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
        id: str | None = None,
        name: str | None = None,
        storage_dir: Path | None = None,
    ) -> RequestQueueClient:
        """Open a request queue client.

        Args:
            id: ID of the queue to open. If not provided, a new queue will be created with a random ID.
            name: Name of the queue to open. If not provided, the queue will be unnamed.
            purge_on_start: If True, the queue will be purged before opening.
            storage_dir: Directory to store the queue data in. If not provided, uses the default storage directory.

        Returns:
            A request queue client.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole request queue and remove all its values.

        The backend method for the `RequestQueue.drop` call.
        """

    @abstractmethod
    async def list_and_lock_head(self, *, lock_secs: int, limit: int | None = None) -> RequestQueueHeadWithLocks:
        """Fetch and lock a specified number of requests from the start of the queue.

        Retrieve and locks the first few requests of a queue for the specified duration. This prevents the requests
        from being fetched by another client until the lock expires.

        Args:
            lock_secs: Duration for which the requests are locked, in seconds.
            limit: Maximum number of requests to retrieve and lock.

        Returns:
            The desired number of locked requests from the beginning of the queue.
        """

    @abstractmethod
    async def add_requests_batch(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> BatchRequestsOperationResponse:
        """Add a requests to the queue in batches.

        Args:
            requests: The batch of requests to add to the queue.
            forefront: Whether to add the requests to the head or the end of the queue.

        Returns:
            Request queue batch operation information.
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
    async def delete_request(self, request_id: str) -> None:
        """Delete a request from the queue.

        Args:
            request_id: ID of the request to delete.
        """

    @abstractmethod
    async def delete_requests_batch(self, requests: list[Request]) -> BatchRequestsOperationResponse:
        """Delete given requests from the queue.

        Args:
            requests: The requests to delete from the queue.
        """

    @abstractmethod
    async def prolong_request_lock(
        self,
        request_id: str,
        *,
        forefront: bool = False,
        lock_secs: int,
    ) -> ProlongRequestLockResponse:
        """Prolong the lock on a specific request in the queue.

        Args:
            request_id: The identifier of the request whose lock is to be prolonged.
            forefront: Whether to put the request in the beginning or the end of the queue after lock expires.
            lock_secs: The additional amount of time, in seconds, that the request will remain locked.
        """

    @abstractmethod
    async def delete_request_lock(
        self,
        request_id: str,
        *,
        forefront: bool = False,
    ) -> None:
        """Delete the lock on a specific request in the queue.

        Args:
            request_id: ID of the request to delete the lock.
            forefront: Whether to put the request in the beginning or the end of the queue after the lock is deleted.
        """
