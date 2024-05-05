from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.models import Request, RequestQueueHead, RequestQueueMetadata, RequestQueueOperationInfo


class BaseRequestQueueClient(ABC):
    """Abstract base class for request queue resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    @abstractmethod
    async def get(self) -> RequestQueueMetadata | None:
        """Get metadata about the request queue being managed by this client.

        Returns:
            An object containing the request queue's details, or None if the request queue does not exist.
        """

    @abstractmethod
    async def update(
        self,
        *,
        name: str | None = None,
    ) -> RequestQueueMetadata:
        """Update the request queue metadata.

        Args:
            name: New new name for the request queue.

        Returns:
            An object reflecting the updated request queue metadata.
        """

    @abstractmethod
    async def delete(self) -> None:
        """Permanently delete the request queue managed by this client."""

    @abstractmethod
    async def list_head(self, *, limit: int | None = None) -> RequestQueueHead:
        """Retrieve a given number of requests from the beginning of the queue.

        Args:
            limit: How many requests to retrieve

        Returns:
            The desired number of requests from the beginning of the queue.
        """

    @abstractmethod
    async def list_and_lock_head(self, *, lock_secs: int, limit: int | None = None) -> dict:
        """Fetch and lock a specified number of requests from the start of the queue.

        Retrieves and locks the first few requests of a queue for the specified duration. This prevents the requests
        from being fetched by another client until the lock expires.

        Args:
            lock_secs: Duration for which the requests are locked, in seconds.
            limit: Maximum number of requests to retrieve and lock.

        Returns:
            The desired number of locked requests from the beginning of the queue.
        """

    @abstractmethod
    async def add_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> RequestQueueOperationInfo:
        """Add a request to the queue.

        Args:
            request: The request to add to the queue
            forefront: Whether to add the request to the head or the end of the queue

        Returns:
            The added request.
        """

    @abstractmethod
    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve

        Returns:
            The retrieved request, or None, if it did not exist.
        """

    @abstractmethod
    async def update_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> RequestQueueOperationInfo:
        """Update a request in the queue.

        Args:
            request: The updated request
            forefront: Whether to put the updated request in the beginning or the end of the queue

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
    async def prolong_request_lock(
        self,
        request_id: str,
        *,
        forefront: bool = False,
        lock_secs: int,
    ) -> dict:
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
            request_id: ID of the request to delete the lock
            forefront: Whether to put the request in the beginning or the end of the queue after the lock is deleted.
        """

    @abstractmethod
    async def batch_add_requests(
        self,
        requests: list[Request],
        *,
        forefront: bool = False,
    ) -> dict:
        """Add batch of requests to the queue.

        Args:
            requests: The requests to add to the queue.
            forefront: Whether to add the requests to the head or the end of the queue.
        """

    @abstractmethod
    async def batch_delete_requests(self, requests: list[Request]) -> dict:
        """Delete given requests from the queue.

        Args:
            requests: The requests to delete from the queue.
        """

    @abstractmethod
    async def list_requests(
        self,
        *,
        limit: int | None = None,
        exclusive_start_id: str | None = None,
    ) -> dict:
        """List requests from the queue.

        Args:
            limit: How many requests to retrieve.
            exclusive_start_id: All requests up to this one (including) are skipped from the result.
        """
