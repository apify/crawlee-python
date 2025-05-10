from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class MemoryRequestQueueClient(RequestQueueClient):
    """Memory implementation of the request queue client.

    This client stores requests in memory using a Python list and dictionary. No data is persisted between
    process runs, which means all requests are lost when the program terminates. This implementation
    is primarily useful for testing, development, and short-lived crawler runs where persistence
    is not required.

    This client provides fast access to request data but is limited by available memory and
    does not support data sharing across different processes.
    """

    def __init__(
        self,
        *,
        id: str,
        name: str | None,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        had_multiple_clients: bool,
        handled_request_count: int,
        pending_request_count: int,
        stats: dict,
        total_request_count: int,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryRequestQueueClient.open` class method to create a new instance.
        """
        self._metadata = RequestQueueMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
            had_multiple_clients=had_multiple_clients,
            handled_request_count=handled_request_count,
            pending_request_count=pending_request_count,
            stats=stats,
            total_request_count=total_request_count,
        )

        # List to hold RQ items
        self._records = list[Request]()

        # Dictionary to track in-progress requests (fetched but not yet handled or reclaimed)
        self._in_progress = dict[str, Request]()

    @override
    @property
    def metadata(self) -> RequestQueueMetadata:
        return self._metadata

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> MemoryRequestQueueClient:
        # Otherwise create a new queue
        queue_id = id or crypto_random_object_id()
        now = datetime.now(timezone.utc)

        return cls(
            id=queue_id,
            name=name,
            created_at=now,
            accessed_at=now,
            modified_at=now,
            had_multiple_clients=False,
            handled_request_count=0,
            pending_request_count=0,
            stats={},
            total_request_count=0,
        )

    @override
    async def drop(self) -> None:
        # Clear all data
        self._records.clear()
        self._in_progress.clear()

        await self._update_metadata(
            update_modified_at=True,
            update_accessed_at=True,
            new_handled_request_count=0,
            new_pending_request_count=0,
            new_total_request_count=0,
        )

    @override
    async def purge(self) -> None:
        """Delete all requests from the queue, but keep the queue itself.

        This method clears all requests including both pending and handled ones,
        but preserves the queue structure.
        """
        self._records.clear()
        self._in_progress.clear()

        await self._update_metadata(
            update_modified_at=True,
            update_accessed_at=True,
            new_handled_request_count=0,
            new_pending_request_count=0,
            new_total_request_count=0,
        )

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        """Add a batch of requests to the queue.

        Args:
            requests: The requests to add.
            forefront: Whether to add the requests to the beginning of the queue.

        Returns:
            Response containing information about the added requests.
        """
        processed_requests = []
        for request in requests:
            # Ensure the request has an ID
            if not request.id:
                request.id = crypto_random_object_id()

            # Check if the request is already in the queue by unique_key
            existing_request = next((r for r in self._records if r.unique_key == request.unique_key), None)

            was_already_present = existing_request is not None
            was_already_handled = was_already_present and existing_request and existing_request.handled_at is not None

            # If the request is already in the queue and handled, don't add it again
            if was_already_handled:
                processed_requests.append(
                    ProcessedRequest(
                        id=request.id,
                        unique_key=request.unique_key,
                        was_already_present=True,
                        was_already_handled=True,
                    )
                )
                continue

            # If the request is already in the queue but not handled, update it
            if was_already_present:
                # Update the existing request with any new data
                for idx, rec in enumerate(self._records):
                    if rec.unique_key == request.unique_key:
                        self._records[idx] = request
                        break
            else:
                # Add the new request to the queue
                if forefront:
                    self._records.insert(0, request)
                else:
                    self._records.append(request)

                # Update metadata counts
                self._metadata.total_request_count += 1
                self._metadata.pending_request_count += 1

            processed_requests.append(
                ProcessedRequest(
                    id=request.id,
                    unique_key=request.unique_key,
                    was_already_present=was_already_present,
                    was_already_handled=False,
                )
            )

        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

        return AddRequestsResponse(
            processed_requests=processed_requests,
            unprocessed_requests=[],
        )

    @override
    async def fetch_next_request(self) -> Request | None:
        """Return the next request in the queue to be processed.

        Returns:
            The request or `None` if there are no more pending requests.
        """
        # Find the first request that's not handled or in progress
        for request in self._records:
            if request.handled_at is None and request.id not in self._in_progress:
                # Mark as in progress
                self._in_progress[request.id] = request
                return request

        return None

    @override
    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve.

        Returns:
            The retrieved request, or None, if it did not exist.
        """
        # Check in-progress requests first
        if request_id in self._in_progress:
            return self._in_progress[request_id]

        # Otherwise search in the records
        for request in self._records:
            if request.id == request_id:
                return request

        return None

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        # Check if the request is in progress
        if request.id not in self._in_progress:
            return None

        # Set handled_at timestamp if not already set
        if request.handled_at is None:
            request.handled_at = datetime.now(timezone.utc)

        # Update the request in records
        for idx, rec in enumerate(self._records):
            if rec.id == request.id:
                self._records[idx] = request
                break

        # Remove from in-progress
        del self._in_progress[request.id]

        # Update metadata counts
        self._metadata.handled_request_count += 1
        self._metadata.pending_request_count -= 1

        # Update metadata timestamps
        await self._update_metadata(update_modified_at=True)

        return ProcessedRequest(
            id=request.id,
            unique_key=request.unique_key,
            was_already_present=True,
            was_already_handled=True,
        )

    @override
    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        """Reclaim a failed request back to the queue.

        The request will be returned for processing later again by another call to `fetch_next_request`.

        Args:
            request: The request to return to the queue.
            forefront: Whether to add the request to the head or the end of the queue.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        # Check if the request is in progress
        if request.id not in self._in_progress:
            return None

        # Remove from in-progress
        del self._in_progress[request.id]

        # If forefront is true, move the request to the beginning of the queue
        if forefront:
            # First remove the request from its current position
            for idx, rec in enumerate(self._records):
                if rec.id == request.id:
                    self._records.pop(idx)
                    break

            # Then insert it at the beginning
            self._records.insert(0, request)

        # Update metadata timestamps
        await self._update_metadata(update_modified_at=True)

        return ProcessedRequest(
            id=request.id,
            unique_key=request.unique_key,
            was_already_present=True,
            was_already_handled=False,
        )

    @override
    async def is_empty(self) -> bool:
        """Check if the queue is empty.

        Returns:
            True if the queue is empty, False otherwise.
        """
        await self._update_metadata(update_accessed_at=True)

        # Queue is empty if there are no pending requests
        pending_requests = [r for r in self._records if r.handled_at is None]
        return len(pending_requests) == 0

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
        new_handled_request_count: int | None = None,
        new_pending_request_count: int | None = None,
        new_total_request_count: int | None = None,
    ) -> None:
        """Update the request queue metadata with current information.

        Args:
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
            new_handled_request_count: If provided, set the handled request count to this value.
            new_pending_request_count: If provided, set the pending request count to this value.
            new_total_request_count: If provided, set the total request count to this value.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now
        if new_handled_request_count is not None:
            self._metadata.handled_request_count = new_handled_request_count
        if new_pending_request_count is not None:
            self._metadata.pending_request_count = new_pending_request_count
        if new_total_request_count is not None:
            self._metadata.total_request_count = new_total_request_count
