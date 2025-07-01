from __future__ import annotations

from collections import deque
from contextlib import suppress
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

logger = getLogger(__name__)


class MemoryRequestQueueClient(RequestQueueClient):
    """Memory implementation of the request queue client.

    No data is persisted between process runs, which means all requests are lost when the program terminates.
    This implementation is primarily useful for testing, development, and short-lived crawler runs where
    persistence is not required.

    This client provides fast access to request data but is limited by available memory and does not support
    data sharing across different processes.
    """

    def __init__(
        self,
        *,
        metadata: RequestQueueMetadata,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryRequestQueueClient.open` class method to create a new instance.
        """
        self._metadata = metadata

        self._pending_requests = deque[Request]()
        """Pending requests are those that have been added to the queue but not yet fetched for processing."""

        self._handled_requests = dict[str, Request]()
        """Handled requests are those that have been processed and marked as handled."""

        self._in_progress_requests = dict[str, Request]()
        """In-progress requests are those that have been fetched but not yet marked as handled or reclaimed."""

        self._requests_by_id = dict[str, Request]()
        """ID -> Request mapping for fast lookup by request ID."""

        self._requests_by_unique_key = dict[str, Request]()
        """Unique key -> Request mapping for fast lookup by unique key."""

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        return self._metadata

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
    ) -> MemoryRequestQueueClient:
        """Open or create a new memory request queue client.

        This method creates a new in-memory request queue instance. Unlike persistent storage implementations,
        memory queues don't check for existing queues with the same name or ID since all data exists only
        in memory and is lost when the process terminates.

        Args:
            id: The ID of the request queue. If not provided, a random ID will be generated.
            name: The name of the request queue. If not provided, the queue will be unnamed.

        Returns:
            An instance for the opened or created storage client.
        """
        # Otherwise create a new queue
        queue_id = id or crypto_random_object_id()
        now = datetime.now(timezone.utc)

        metadata = RequestQueueMetadata(
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

        return cls(metadata=metadata)

    @override
    async def drop(self) -> None:
        self._pending_requests.clear()
        self._handled_requests.clear()
        self._requests_by_id.clear()
        self._requests_by_unique_key.clear()
        self._in_progress_requests.clear()

        await self._update_metadata(
            update_modified_at=True,
            update_accessed_at=True,
            new_handled_request_count=0,
            new_pending_request_count=0,
            new_total_request_count=0,
        )

    @override
    async def purge(self) -> None:
        self._pending_requests.clear()
        self._handled_requests.clear()
        self._requests_by_id.clear()
        self._requests_by_unique_key.clear()
        self._in_progress_requests.clear()

        await self._update_metadata(
            update_modified_at=True,
            update_accessed_at=True,
            new_pending_request_count=0,
        )

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        processed_requests = []
        for request in requests:
            # Check if the request is already in the queue by unique_key.
            existing_request = self._requests_by_unique_key.get(request.unique_key)

            was_already_present = existing_request is not None
            was_already_handled = was_already_present and existing_request and existing_request.handled_at is not None

            # If the request is already in the queue and handled, don't add it again.
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

            # If the request is already in the queue but not handled, update it.
            if was_already_present and existing_request:
                # Update the existing request with any new data and
                # remove old request from pending queue if it's there.
                with suppress(ValueError):
                    self._pending_requests.remove(existing_request)

                # Update indexes.
                self._requests_by_id[request.id] = request
                self._requests_by_unique_key[request.unique_key] = request

                # Add updated request back to queue.
                if forefront:
                    self._pending_requests.appendleft(request)
                else:
                    self._pending_requests.append(request)
            # Add the new request to the queue.
            else:
                if forefront:
                    self._pending_requests.appendleft(request)
                else:
                    self._pending_requests.append(request)

                # Update indexes.
                self._requests_by_id[request.id] = request
                self._requests_by_unique_key[request.unique_key] = request

                await self._update_metadata(
                    new_total_request_count=self._metadata.total_request_count + 1,
                    new_pending_request_count=self._metadata.pending_request_count + 1,
                )

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
        while self._pending_requests:
            request = self._pending_requests.popleft()

            # Skip if already handled (shouldn't happen, but safety check).
            if request.was_already_handled:
                continue

            # Skip if already in progress (shouldn't happen, but safety check).
            if request.id in self._in_progress_requests:
                self._pending_requests.appendleft(request)
                break

            # Mark as in progress.
            self._in_progress_requests[request.id] = request
            return request

        return None

    @override
    async def get_request(self, request_id: str) -> Request | None:
        await self._update_metadata(update_accessed_at=True)
        return self._requests_by_id.get(request_id)

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        # Check if the request is in progress.
        if request.id not in self._in_progress_requests:
            return None

        # Set handled_at timestamp if not already set.
        if not request.was_already_handled:
            request.handled_at = datetime.now(timezone.utc)

        # Move request to handled storage.
        self._handled_requests[request.id] = request

        # Update indexes (keep the request in indexes for get_request to work).
        self._requests_by_id[request.id] = request
        self._requests_by_unique_key[request.unique_key] = request

        # Remove from in-progress.
        del self._in_progress_requests[request.id]

        # Update metadata.
        await self._update_metadata(
            new_handled_request_count=self._metadata.handled_request_count + 1,
            new_pending_request_count=self._metadata.pending_request_count - 1,
            update_modified_at=True,
        )

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
        # Check if the request is in progress.
        if request.id not in self._in_progress_requests:
            return None

        # Remove from in-progress.
        del self._in_progress_requests[request.id]

        # Add request back to pending queue.
        if forefront:
            self._pending_requests.appendleft(request)
        else:
            self._pending_requests.append(request)

        # Update metadata timestamps.
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

        # Queue is empty if there are no pending requests and no requests in progress.
        return len(self._pending_requests) == 0 and len(self._in_progress_requests) == 0

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
