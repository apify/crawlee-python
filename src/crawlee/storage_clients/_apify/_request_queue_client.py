from __future__ import annotations

import asyncio
import os
from collections import deque
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar, Final

from apify_client import ApifyClientAsync
from cachetools import LRUCache
from typing_extensions import override

from crawlee import Request
from crawlee._utils.requests import unique_key_to_request_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    CachedRequest,
    ProcessedRequest,
    ProlongRequestLockResponse,
    RequestQueueHead,
    RequestQueueMetadata,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from apify_client.clients import RequestQueueClientAsync

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class ApifyRequestQueueClient(RequestQueueClient):
    """An Apify platform implementation of the request queue client."""

    _cache_by_name: ClassVar[dict[str, ApifyRequestQueueClient]] = {}
    """A dictionary to cache clients by their names."""

    _DEFAULT_LOCK_TIME: Final[timedelta] = timedelta(minutes=3)
    """The default lock time for requests in the queue."""

    _MAX_CACHED_REQUESTS: Final[int] = 1_000_000
    """Maximum number of requests that can be cached."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        had_multiple_clients: bool,
        handled_request_count: int,
        pending_request_count: int,
        stats: dict,
        total_request_count: int,
        api_client: RequestQueueClientAsync,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `ApifyRequestQueueClient.open` class method to create a new instance.
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

        self._api_client = api_client
        """The Apify request queue client for API operations."""

        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

        self._queue_head = deque[str]()
        """A deque to store request IDs in the queue head."""

        self._requests_cache: LRUCache[str, CachedRequest] = LRUCache(maxsize=self._MAX_CACHED_REQUESTS)
        """A cache to store request objects."""

        self._queue_has_locked_requests: bool | None = None
        """Whether the queue has requests locked by another client."""

        self._should_check_for_forefront_requests = False
        """Whether to check for forefront requests in the next list_head call."""

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
    ) -> ApifyRequestQueueClient:
        default_name = configuration.default_request_queue_id

        # Get API credentials
        token = os.environ.get('APIFY_TOKEN')
        api_url = 'https://api.apify.com'

        name = name or default_name

        # Check if the client is already cached by name.
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata()  # noqa: SLF001
            return client

        # Create a new API client
        apify_client_async = ApifyClientAsync(
            token=token,
            api_url=api_url,
            max_retries=8,
            min_delay_between_retries_millis=500,
            timeout_secs=360,
        )

        apify_rqs_client = apify_client_async.request_queues()

        # Get or create the request queue
        metadata = RequestQueueMetadata.model_validate(
            await apify_rqs_client.get_or_create(name=id if id is not None else name),
        )

        apify_rq_client = apify_client_async.request_queue(request_queue_id=metadata.id)

        # Create the client instance
        client = cls(
            id=metadata.id,
            name=metadata.name,
            created_at=metadata.created_at,
            accessed_at=metadata.accessed_at,
            modified_at=metadata.modified_at,
            had_multiple_clients=metadata.had_multiple_clients,
            handled_request_count=metadata.handled_request_count,
            pending_request_count=metadata.pending_request_count,
            stats=metadata.stats,
            total_request_count=metadata.total_request_count,
            api_client=apify_rq_client,
        )

        # Cache the client by name
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        async with self._lock:
            await self._api_client.delete()

            # Remove the client from the cache
            if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
                del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

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
        # Prepare requests for API by converting to dictionaries
        requests_dict = [request.model_dump(by_alias=True) for request in requests]

        # Remove 'id' fields from requests as the API doesn't accept them
        for request_dict in requests_dict:
            if 'id' in request_dict:
                del request_dict['id']

        # Send requests to API
        response = await self._api_client.batch_add_requests(requests=requests_dict, forefront=forefront)

        # Update metadata after adding requests
        await self._update_metadata()

        return AddRequestsResponse.model_validate(response)

    @override
    async def get_request(self, request_id: str) -> Request | None:
        """Get a request by ID.

        Args:
            request_id: The ID of the request to get.

        Returns:
            The request or None if not found.
        """
        response = await self._api_client.get_request(request_id)
        await self._update_metadata()

        if response is None:
            return None

        return Request.model_validate(**response)

    @override
    async def fetch_next_request(self) -> Request | None:
        """Return the next request in the queue to be processed.

        Once you successfully finish processing of the request, you need to call `mark_request_as_handled`
        to mark the request as handled in the queue. If there was some error in processing the request, call
        `reclaim_request` instead, so that the queue will give the request to some other consumer
        in another call to the `fetch_next_request` method.

        Returns:
            The request or `None` if there are no more pending requests.
        """
        # Ensure the queue head has requests if available
        await self._ensure_head_is_non_empty()

        # If queue head is empty after ensuring, there are no requests
        if not self._queue_head:
            return None

        # Get the next request ID from the queue head
        next_request_id = self._queue_head.popleft()
        request = await self._get_or_hydrate_request(next_request_id)

        # Handle potential inconsistency where request might not be in the main table yet
        if request is None:
            logger.debug(
                'Cannot find a request from the beginning of queue, will be retried later',
                extra={'nextRequestId': next_request_id},
            )
            return None

        # If the request was already handled, skip it
        if request.handled_at is not None:
            logger.debug(
                'Request fetched from the beginning of queue was already handled',
                extra={'nextRequestId': next_request_id},
            )
            return None

        return request

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        # Set the handled_at timestamp if not already set
        if request.handled_at is None:
            request.handled_at = datetime.now(tz=timezone.utc)

        try:
            # Update the request in the API
            processed_request = await self._update_request(request)
            processed_request.unique_key = request.unique_key

            # Update the cache with the handled request
            cache_key = unique_key_to_request_id(request.unique_key)
            self._cache_request(
                cache_key,
                processed_request,
                forefront=False,
                hydrated_request=request,
            )

            # Update metadata after marking request as handled
            await self._update_metadata()
        except Exception as exc:
            logger.debug(f'Error marking request {request.id} as handled: {exc!s}')
            return None
        else:
            return processed_request

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
        try:
            # Update the request in the API
            processed_request = await self._update_request(request, forefront=forefront)
            processed_request.unique_key = request.unique_key

            # Update the cache
            cache_key = unique_key_to_request_id(request.unique_key)
            self._cache_request(
                cache_key,
                processed_request,
                forefront=forefront,
                hydrated_request=request,
            )

            # If we're adding to the forefront, we need to check for forefront requests
            # in the next list_head call
            if forefront:
                self._should_check_for_forefront_requests = True

            # Try to release the lock on the request
            try:
                await self._delete_request_lock(request.id, forefront=forefront)
            except Exception as err:
                logger.debug(f'Failed to delete request lock for request {request.id}', exc_info=err)

            # Update metadata after reclaiming request
            await self._update_metadata()
        except Exception as exc:
            logger.debug(f'Error reclaiming request {request.id}: {exc!s}')
            return None
        else:
            return processed_request

    @override
    async def is_empty(self) -> bool:
        """Check if the queue is empty.

        Returns:
            True if the queue is empty, False otherwise.
        """
        head = await self._list_head(limit=1, lock_time=None)
        return len(head.items) == 0

    async def _ensure_head_is_non_empty(self) -> None:
        """Ensure that the queue head has requests if they are available in the queue."""
        # If queue head has adequate requests, skip fetching more
        if len(self._queue_head) > 1 and not self._should_check_for_forefront_requests:
            return

        # Fetch requests from the API and populate the queue head
        await self._list_head(lock_time=self._DEFAULT_LOCK_TIME)

    async def _get_or_hydrate_request(self, request_id: str) -> Request | None:
        """Get a request by ID, either from cache or by fetching from API.

        Args:
            request_id: The ID of the request to get.

        Returns:
            The request if found and valid, otherwise None.
        """
        # First check if the request is in our cache
        cached_entry = self._requests_cache.get(request_id)

        if cached_entry and cached_entry.hydrated:
            # If we have the request hydrated in cache, check if lock is expired
            if cached_entry.lock_expires_at and cached_entry.lock_expires_at < datetime.now(tz=timezone.utc):
                # Try to prolong the lock if it's expired
                try:
                    lock_secs = int(self._DEFAULT_LOCK_TIME.total_seconds())
                    response = await self._prolong_request_lock(
                        request_id, forefront=cached_entry.forefront, lock_secs=lock_secs
                    )
                    cached_entry.lock_expires_at = response.lock_expires_at
                except Exception:
                    # If prolonging the lock fails, we lost the request
                    logger.debug(f'Failed to prolong lock for request {request_id}, returning None')
                    return None

            return cached_entry.hydrated

        # If not in cache or not hydrated, fetch the request
        try:
            # Try to acquire or prolong the lock
            lock_secs = int(self._DEFAULT_LOCK_TIME.total_seconds())
            await self._prolong_request_lock(request_id, forefront=False, lock_secs=lock_secs)

            # Fetch the request data
            request = await self.get_request(request_id)

            # If request is not found, release lock and return None
            if not request:
                await self._delete_request_lock(request_id)
                return None

            # Update cache with hydrated request
            cache_key = unique_key_to_request_id(request.unique_key)
            self._cache_request(
                cache_key,
                ProcessedRequest(
                    id=request_id,
                    unique_key=request.unique_key,
                    was_already_present=True,
                    was_already_handled=request.handled_at is not None,
                ),
                forefront=False,
                hydrated_request=request,
            )
        except Exception as exc:
            logger.debug(f'Error fetching or locking request {request_id}: {exc!s}')
            return None
        else:
            return request

    async def _update_request(
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
        response = await self._api_client.update_request(
            request=request.model_dump(by_alias=True),
            forefront=forefront,
        )

        return ProcessedRequest.model_validate(
            {'id': request.id, 'uniqueKey': request.unique_key} | response,
        )

    async def _list_head(
        self,
        *,
        lock_time: timedelta | None = None,
        limit: int = 25,
    ) -> RequestQueueHead:
        """Retrieve requests from the beginning of the queue.

        Args:
            lock_time: Duration for which to lock the retrieved requests.
                If None, requests will not be locked.
            limit: Maximum number of requests to retrieve.

        Returns:
            A collection of requests from the beginning of the queue.
        """
        # Return from cache if available and we're not checking for new forefront requests
        if self._queue_head and not self._should_check_for_forefront_requests:
            logger.debug(f'Using cached queue head with {len(self._queue_head)} requests')

            # Create a list of requests from the cached queue head
            items = []
            for request_id in list(self._queue_head)[:limit]:
                cached_request = self._requests_cache.get(request_id)
                if cached_request and cached_request.hydrated:
                    items.append(cached_request.hydrated)

            return RequestQueueHead(
                limit=limit,
                had_multiple_clients=self._metadata.had_multiple_clients,
                queue_modified_at=self._metadata.modified_at,
                items=items,
                queue_has_locked_requests=self._queue_has_locked_requests,
                lock_time=lock_time,
            )

        # Otherwise fetch from API
        lock_time = lock_time or self._DEFAULT_LOCK_TIME
        lock_secs = int(lock_time.total_seconds())

        response = await self._api_client.list_and_lock_head(
            lock_secs=lock_secs,
            limit=limit,
        )

        # Update the queue head cache
        self._queue_has_locked_requests = response.get('queueHasLockedRequests', False)

        # Clear current queue head if we're checking for forefront requests
        if self._should_check_for_forefront_requests:
            self._queue_head.clear()
            self._should_check_for_forefront_requests = False

        # Process and cache the requests
        head_id_buffer = list[str]()
        forefront_head_id_buffer = list[str]()

        for request_data in response.get('items', []):
            request = Request.model_validate(request_data)

            # Skip requests without ID or unique key
            if not request.id or not request.unique_key:
                logger.debug(
                    'Skipping request from queue head, missing ID or unique key',
                    extra={
                        'id': request.id,
                        'unique_key': request.unique_key,
                    },
                )
                continue

            # Check if this request was already cached and if it was added to forefront
            cache_key = unique_key_to_request_id(request.unique_key)
            cached_request = self._requests_cache.get(cache_key)
            forefront = cached_request.forefront if cached_request else False

            # Add to appropriate buffer based on forefront flag
            if forefront:
                forefront_head_id_buffer.insert(0, request.id)
            else:
                head_id_buffer.append(request.id)

            # Cache the request
            self._cache_request(
                cache_key,
                ProcessedRequest(
                    id=request.id,
                    unique_key=request.unique_key,
                    was_already_present=True,
                    was_already_handled=False,
                ),
                forefront=forefront,
                hydrated_request=request,
            )

        # Update the queue head deque
        for request_id in head_id_buffer:
            self._queue_head.append(request_id)

        for request_id in forefront_head_id_buffer:
            self._queue_head.appendleft(request_id)

        return RequestQueueHead.model_validate(response)

    async def _prolong_request_lock(
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

        Returns:
            A response containing the time at which the lock will expire.
        """
        response = await self._api_client.prolong_request_lock(
            request_id=request_id,
            forefront=forefront,
            lock_secs=lock_secs,
        )

        result = ProlongRequestLockResponse(
            lock_expires_at=datetime.fromisoformat(response['lockExpiresAt'].replace('Z', '+00:00'))
        )

        # Update the cache with the new lock expiration
        for cached_request in self._requests_cache.values():
            if cached_request.id == request_id:
                cached_request.lock_expires_at = result.lock_expires_at
                break

        return result

    async def _delete_request_lock(
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
        try:
            await self._api_client.delete_request_lock(
                request_id=request_id,
                forefront=forefront,
            )

            # Update the cache to remove the lock
            for cached_request in self._requests_cache.values():
                if cached_request.id == request_id:
                    cached_request.lock_expires_at = None
                    break
        except Exception as err:
            logger.debug(f'Failed to delete request lock for request {request_id}', exc_info=err)

    def _cache_request(
        self,
        cache_key: str,
        processed_request: ProcessedRequest,
        *,
        forefront: bool,
        hydrated_request: Request | None = None,
    ) -> None:
        """Cache a request for future use.

        Args:
            cache_key: The key to use for caching the request.
            processed_request: The processed request information.
            forefront: Whether the request was added to the forefront of the queue.
            hydrated_request: The hydrated request object, if available.
        """
        self._requests_cache[cache_key] = CachedRequest(
            id=processed_request.id,
            was_already_handled=processed_request.was_already_handled,
            hydrated=hydrated_request,
            lock_expires_at=None,
            forefront=forefront,
        )

    async def _update_metadata(self) -> None:
        """Update the request queue metadata with current information."""
        metadata = await self._api_client.get()
        self._metadata = RequestQueueMetadata.model_validate(metadata)
