from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from apify_client import ApifyClientAsync
from cachetools import LRUCache
from typing_extensions import override

from crawlee import Request
from crawlee._utils.requests import unique_key_to_request_id
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.events import Event
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    ProcessedRequest,
    ProlongRequestLockResponse,
    RequestQueueHead,
    RequestQueueMetadata,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from apify_client.clients import RequestQueueClientAsync

    from crawlee.configuration import Configuration
    from crawlee.events import EventManager
    from crawlee.storages._types import CachedRequest

logger = getLogger(__name__)

_API_TOKEN = 'apify_api_Z9PfLfYya1llJGUlunDN15YVl4uo8r40hVCV'
_API_URL = 'https://api.apify.com'

T = TypeVar('T')


class ApifyRequestQueueClient(RequestQueueClient):
    """An Apify platform implementation of the request queue client."""

    _cache_by_name: ClassVar[dict[str, ApifyRequestQueueClient]] = {}
    """A dictionary to cache clients by their names."""

    _DEFAULT_LOCK_TIME = timedelta(minutes=3)
    """The default lock time for requests in the queue."""

    _MAX_CACHED_REQUESTS = 1_000_000
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
        event_manager: EventManager | None = None,
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
        """The Apify key-value store client for API operations."""

        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

        self._add_requests_tasks = list[asyncio.Task]()
        """A list of tasks for adding requests to the queue."""

        self._assumed_total_count = 0
        """An assumed total count of requests in the queue."""

        self._assumed_handled_count = 0
        """An assumed count of handled requests in the queue."""

        # Internal state
        self._request_lock_time = timedelta(minutes=3)
        self._queue_paused_for_migration = False
        self._queue_has_locked_requests: bool | None = None
        self._should_check_for_forefront_requests = False
        self._is_finished_log_throttle_counter = 0
        self._dequeued_request_count = 0
        self._tasks = list[asyncio.Task]()
        self._client_key = ''  # Will be set by the client
        self._queue_head = deque[str]()
        self._list_head_and_lock_task: asyncio.Task | None = None
        self._last_activity = datetime.now(timezone.utc)
        self._requests_cache: LRUCache[str, CachedRequest] = LRUCache(maxsize=self._MAX_CACHED_REQUESTS)

        # Event handling
        self._event_manager = event_manager
        if event_manager:
            event_manager.on(
                event=Event.MIGRATING,
                listener=lambda _: setattr(self, '_queue_paused_for_migration', True),
            )
            event_manager.on(
                event=Event.MIGRATING,
                listener=self._clear_possible_locks,
            )
            event_manager.on(
                event=Event.ABORTING,
                listener=self._clear_possible_locks,
            )

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
        token = _API_TOKEN  # TODO: use the real value
        api_url = _API_URL  # TODO: use the real value

        name = name or default_name

        # Check if the client is already cached by name.
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata()  # noqa: SLF001
            return client

        # Otherwise, create a new one.
        apify_client_async = ApifyClientAsync(
            token=token,
            api_url=api_url,
            max_retries=8,
            min_delay_between_retries_millis=500,
            timeout_secs=360,
        )

        apify_rqs_client = apify_client_async.request_queues()

        metadata = RequestQueueMetadata.model_validate(
            await apify_rqs_client.get_or_create(name=id if id is not None else name),
        )

        apify_rq_client = apify_client_async.request_queue(request_queue_id=metadata.id)

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

        # Cache the client by name.
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        async with self._lock:
            await self._api_client.delete()

            # Remove the client from the cache.
            if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
                del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

    @override
    async def list_head(
        self,
        *,
        lock_time: timedelta | None = None,
        limit: int | None = None,
    ) -> RequestQueueHead:
        lock_time = lock_time or self._DEFAULT_LOCK_TIME

        response = await self._api_client.list_and_lock_head(
            lock_secs=int(lock_time.total_seconds()),
            limit=limit,
        )

        return RequestQueueHead.model_validate(**response)

    @override
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
        wait_time_secs = wait_time_between_batches.total_seconds()

        async def _process_batch(batch: Sequence[Request]) -> None:
            request_count = len(batch)
            requests_dict = [request.model_dump(by_alias=True) for request in batch]
            response = await self._api_client.batch_add_requests(requests=requests_dict, forefront=forefront)
            self._assumed_total_count += request_count
            logger.debug(f'Added {request_count} requests to the queue, response: {response}')

        # Wait for the first batch to be added
        first_batch = requests[:batch_size]
        if first_batch:
            await _process_batch(first_batch)

        async def _process_remaining_batches() -> None:
            for i in range(batch_size, len(requests), batch_size):
                batch = requests[i : i + batch_size]
                await _process_batch(batch)
                if i + batch_size < len(requests):
                    await asyncio.sleep(wait_time_secs)

        # Create and start the task to process remaining batches in the background
        remaining_batches_task = asyncio.create_task(
            _process_remaining_batches(),
            name='request_queue_process_remaining_batches_task',
        )
        self._add_requests_tasks.append(remaining_batches_task)
        remaining_batches_task.add_done_callback(lambda _: self._add_requests_tasks.remove(remaining_batches_task))

        # Wait for all tasks to finish if requested
        if wait_for_all_requests_to_be_added:
            await wait_for_all_tasks_for_finish(
                (remaining_batches_task,),
                logger=logger,
                timeout=wait_for_all_requests_to_be_added_timeout,
            )

        response = await self._api_client.batch_add_requests(
            requests=[request.model_dump(by_alias=True, exclude={'id'}) for request in requests],
            forefront=forefront,
        )

        result = AddRequestsResponse.model_validate(response)

        # Cache the processed requests
        for processed_request in result.processed_requests:
            self._cache_request(
                unique_key_to_request_id(processed_request.unique_key),
                processed_request,
                forefront=forefront,
            )

        return result

    @override
    async def get_request(self, request_id: str) -> Request | None:
        response = await self._api_client.get_request(request_id)
        if response is None:
            return None
        return Request.model_validate(**response)

    @override
    async def update_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        response = await self._api_client.update_request(
            request=request.model_dump(by_alias=True),
            forefront=forefront,
        )

        return ProcessedRequest.model_validate(
            {'id': request.id, 'uniqueKey': request.unique_key} | response,
        )

    @override
    async def is_finished(self) -> bool:
        if self._tasks:
            logger.debug('Background tasks are still in progress')
            return False

        if self._queue_head:
            logger.debug(
                'There are still ids in the queue head that are pending processing',
                extra={
                    'queue_head_ids_pending': len(self._queue_head),
                },
            )

            return False

        await self._ensure_head_is_non_empty()

        if self._queue_head:
            logger.debug('Queue head still returned requests that need to be processed')

            return False

        # Could not lock any new requests - decide based on whether the queue contains requests locked by another client
        if self._queue_has_locked_requests is not None:
            if self._queue_has_locked_requests and self._dequeued_request_count == 0:
                # The `% 25` was absolutely arbitrarily picked. It's just to not spam the logs too much.
                if self._is_finished_log_throttle_counter % 25 == 0:
                    logger.info('The queue still contains requests locked by another client')

                self._is_finished_log_throttle_counter += 1

            logger.debug(
                f'Deciding if we are finished based on `queue_has_locked_requests` = {self._queue_has_locked_requests}'
            )
            return not self._queue_has_locked_requests

        metadata = await self.get()
        if metadata is not None and not metadata.had_multiple_clients and not self._queue_head:
            logger.debug('Queue head is empty and there are no other clients - we are finished')

            return True

        # The following is a legacy algorithm for checking if the queue is finished.
        # It is used only for request queue clients that do not provide the `queue_has_locked_requests` flag.
        current_head = await self.list_head(limit=2)

        if current_head.items:
            logger.debug('The queue still contains unfinished requests or requests locked by another client')

        return len(current_head.items) == 0

    async def get(self) -> RequestQueueMetadata | None:
        """Get an object containing general information about the request queue."""
        response = await self._api_client.get()
        if response is None:
            return None
        self._metadata = RequestQueueMetadata.model_validate(response)
        return self._metadata

    async def prolong_request_lock(
        self,
        request_id: str,
        *,
        lock_secs: int,
    ) -> ProlongRequestLockResponse | None:
        """Prolong the lock on a specific request in the queue.

        Args:
            request_id: The identifier of the request whose lock is to be prolonged.
            lock_secs: The additional amount of time, in seconds, that the request will remain locked.

        Returns:
            Response containing the lock expiration time or None if the operation failed.
        """
        try:
            response = await self._api_client.prolong_request_lock(
                request_id=request_id,
                lock_secs=lock_secs,
                forefront=False,  # Default value
            )
            return ProlongRequestLockResponse.model_validate(response)
        except Exception as err:
            # Most likely we do not own the lock anymore
            logger.warning(
                f'Failed to prolong lock for cached request {request_id}, either lost the lock '
                'or the request was already handled\n',
                exc_info=err,
            )
            return None

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
        try:
            await self._api_client.delete_request_lock(
                request_id=request_id,
                forefront=forefront,
            )
        except Exception as err:
            logger.debug(f'Failed to delete request lock for request {request_id}', exc_info=err)

    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        self._last_activity = datetime.now(timezone.utc)

        if request.handled_at is None:
            request.handled_at = datetime.now(timezone.utc)

        processed_request = await self.update_request(request)
        processed_request.unique_key = request.unique_key
        self._dequeued_request_count -= 1

        if not processed_request.was_already_handled:
            self._assumed_handled_count += 1

        self._cache_request(unique_key_to_request_id(request.unique_key), processed_request, forefront=False)
        return processed_request

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
        self._last_activity = datetime.now(timezone.utc)

        processed_request = await self.update_request(request, forefront=forefront)
        processed_request.unique_key = request.unique_key
        self._cache_request(unique_key_to_request_id(request.unique_key), processed_request, forefront=forefront)

        if forefront:
            self._should_check_for_forefront_requests = True

        if processed_request:
            # Try to delete the request lock if possible
            try:
                await self.delete_request_lock(request.id, forefront=forefront)
            except Exception as err:
                logger.debug(f'Failed to delete request lock for request {request.id}', exc_info=err)

        return processed_request

    async def is_empty(self) -> bool:
        """Check whether the queue is empty.

        Returns:
            bool: `True` if the next call to `fetch_next_request` would return `None`, otherwise `False`.
        """
        await self._ensure_head_is_non_empty()
        return len(self._queue_head) == 0

    async def fetch_next_request(self) -> Request | None:
        """Return the next request in the queue to be processed.

        Once you successfully finish processing of the request, you need to call `mark_request_as_handled`
        to mark the request as handled in the queue. If there was some error in processing the request, call
        `reclaim_request` instead, so that the queue will give the request to some other consumer
        in another call to the `fetch_next_request` method.

        Note that the `None` return value does not mean the queue processing finished, it means there are currently
        no pending requests. To check whether all requests in queue were finished, use `is_finished`
        instead.

        Returns:
            The request or `None` if there are no more pending requests.
        """
        self._last_activity = datetime.now(timezone.utc)

        await self._ensure_head_is_non_empty()

        # We are likely done at this point.
        if len(self._queue_head) == 0:
            return None

        next_request_id = self._queue_head.popleft()
        request = await self._get_or_hydrate_request(next_request_id)

        # NOTE: It can happen that the queue head index is inconsistent with the main queue table.
        # This can occur in two situations:

        # 1)
        # Queue head index is ahead of the main table and the request is not present in the main table yet
        # (i.e. get_request() returned null). In this case, keep the request marked as in progress for a short while,
        # so that is_finished() doesn't return true and _ensure_head_is_non_empty() doesn't not load the request into
        # the queueHeadDict straight again. After the interval expires, fetch_next_request() will try to fetch this
        # request again, until it eventually appears in the main table.
        if request is None:
            logger.debug(
                'Cannot find a request from the beginning of queue, will be retried later',
                extra={'nextRequestId': next_request_id},
            )
            return None

        # 2)
        # Queue head index is behind the main table and the underlying request was already handled (by some other
        # client, since we keep the track of handled requests in recently_handled dictionary). We just add the request
        # to the recently_handled dictionary so that next call to _ensure_head_is_non_empty() will not put the request
        # again to queue_head_dict.
        if request.handled_at is not None:
            logger.debug(
                'Request fetched from the beginning of queue was already handled',
                extra={'nextRequestId': next_request_id},
            )
            return None

        self._dequeued_request_count += 1
        return request

    async def get_handled_count(self) -> int:
        """Get the number of handled requests."""
        return self._assumed_handled_count

    async def get_total_count(self) -> int:
        """Get the total number of requests."""
        return self._assumed_total_count

    def _reset(self) -> None:
        """Reset the internal state of the client."""
        self._queue_head.clear()
        self._list_head_and_lock_task = None
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._requests_cache.clear()
        self._last_activity = datetime.now(timezone.utc)

    async def _update_metadata(self) -> None:
        """Update the request queue metadata with current information."""
        metadata = await self.get()
        if metadata:
            self._metadata = metadata

    async def _list_head_and_lock(self) -> None:
        """List the head of the queue and lock the requests."""
        # Make a copy so that we can clear the flag only if the whole method executes after the flag was set
        # (i.e, it was not set in the middle of the execution of the method)
        should_check_for_forefront_requests = self._should_check_for_forefront_requests

        limit = 25

        response = await self._api_client.list_and_lock_head(
            limit=limit,
            lock_secs=int(self._request_lock_time.total_seconds()),
        )
        print(f'response = {response}')

        head = RequestQueueHead.model_validate(**response)

        self._queue_has_locked_requests = response.get('queueHasLockedRequests', False)

        head_id_buffer = list[str]()
        forefront_head_id_buffer = list[str]()

        for request in head.items:
            # Queue head index might be behind the main table, so ensure we don't recycle requests
            if not request.id or not request.unique_key:
                logger.debug(
                    'Skipping request from queue head, already in progress or recently handled',
                    extra={
                        'id': request.id,
                        'unique_key': request.unique_key,
                    },
                )

                # Remove the lock from the request for now, so that it can be picked up later
                # This may/may not succeed, but that's fine
                with suppress(Exception):
                    await self.delete_request_lock(request.id)

                continue

            # If we remember that we added the request ourselves and we added it to the forefront,
            # we will put it to the beginning of the local queue head to preserve the expected order.
            # If we do not remember that, we will enqueue it normally.
            cached_request = self._requests_cache.get(unique_key_to_request_id(request.unique_key))
            forefront = cached_request.get('forefront', False) if cached_request else False

            if forefront:
                forefront_head_id_buffer.insert(0, request.id)
            else:
                head_id_buffer.append(request.id)

            self._cache_request(
                unique_key_to_request_id(request.unique_key),
                ProcessedRequest(
                    id=request.id,
                    unique_key=request.unique_key,
                    was_already_present=True,
                    was_already_handled=False,
                ),
                forefront=forefront,
            )

        for request_id in head_id_buffer:
            self._queue_head.append(request_id)

        for request_id in forefront_head_id_buffer:
            self._queue_head.appendleft(request_id)

        # If the queue head became too big, unlock the excess requests
        to_unlock = list[str]()
        while len(self._queue_head) > limit:
            to_unlock.append(self._queue_head.pop())

        if to_unlock:
            await asyncio.gather(
                *[self.delete_request_lock(request_id) for request_id in to_unlock],
                return_exceptions=True,  # Just ignore the exceptions
            )

        # Unset the should_check_for_forefront_requests flag - the check is finished
        if should_check_for_forefront_requests:
            self._should_check_for_forefront_requests = False

    async def _ensure_head_is_non_empty(self) -> None:
        """Ensure that the queue head is non-empty."""
        # Stop fetching if we are paused for migration
        if self._queue_paused_for_migration:
            return

        # We want to fetch ahead of time to minimize dead time
        if len(self._queue_head) > 1 and not self._should_check_for_forefront_requests:
            return

        if self._list_head_and_lock_task is None:
            task = asyncio.create_task(self._list_head_and_lock(), name='request_queue_list_head_and_lock_task')

            def callback(_: Any) -> None:
                self._list_head_and_lock_task = None

            task.add_done_callback(callback)
            self._list_head_and_lock_task = task

        await self._list_head_and_lock_task

    async def _get_or_hydrate_request(self, request_id: str) -> Request | None:
        """Get or hydrate a request from the cache or the API."""
        cached_entry = self._requests_cache.get(request_id)

        if not cached_entry:
            # 2.1. Attempt to prolong the request lock to see if we still own the request
            prolong_result = await self.prolong_request_lock(
                request_id, lock_secs=int(self._request_lock_time.total_seconds())
            )

            if not prolong_result:
                return None

            # 2.1.1. If successful, hydrate the request and return it
            hydrated_request = await self.get_request(request_id)

            # Queue head index is ahead of the main table and the request is not present in the main table yet
            # (i.e. get_request() returned null).
            if not hydrated_request:
                # Remove the lock from the request for now, so that it can be picked up later
                # This may/may not succeed, but that's fine
                with suppress(Exception):
                    await self.delete_request_lock(request_id)

                return None

            self._requests_cache[request_id] = {
                'id': request_id,
                'hydrated': hydrated_request,
                'was_already_handled': hydrated_request.handled_at is not None,
                'lock_expires_at': prolong_result.lock_expires_at,
                'forefront': False,
            }

            return hydrated_request

        # 1.1. If hydrated, prolong the lock more and return it
        if cached_entry.get('hydrated'):
            # 1.1.1. If the lock expired on the hydrated requests, try to prolong. If we fail, we lost the request
            # (or it was handled already)
            lock_expires_at = cached_entry.get('lock_expires_at')
            if lock_expires_at and lock_expires_at < datetime.now(timezone.utc):
                prolonged = await self.prolong_request_lock(
                    cached_entry.get('id', ''), lock_secs=int(self._request_lock_time.total_seconds())
                )

                if not prolonged:
                    return None

                cached_entry['lock_expires_at'] = prolonged.lock_expires_at

            return cached_entry.get('hydrated')

        # 1.2. If not hydrated, try to prolong the lock first (to ensure we keep it in our queue), hydrate and return it
        prolonged = await self.prolong_request_lock(
            cached_entry.get('id', ''), lock_secs=int(self._request_lock_time.total_seconds())
        )

        if not prolonged:
            return None

        # This might still return null if the queue head is inconsistent with the main queue table.
        hydrated_request = await self.get_request(cached_entry.get('id', ''))

        cached_entry['hydrated'] = hydrated_request

        # Queue head index is ahead of the main table and the request is not present in the main table yet
        # (i.e. get_request() returned null).
        if not hydrated_request:
            # Remove the lock from the request for now, so that it can be picked up later
            # This may/may not succeed, but that's fine
            with suppress(Exception):
                await self.delete_request_lock(cached_entry.get('id', ''))

            return None

        return hydrated_request

    def _cache_request(self, cache_key: str, processed_request: ProcessedRequest, *, forefront: bool) -> None:
        """Cache a request for future use."""
        self._requests_cache[cache_key] = {
            'id': processed_request.id,
            'was_already_handled': processed_request.was_already_handled,
            'hydrated': None,
            'lock_expires_at': None,
            'forefront': forefront,
        }

    async def _clear_possible_locks(self) -> None:
        """Clear any possible locks in the queue."""
        self._queue_paused_for_migration = True
        request_id: str | None = None

        while True:
            try:
                request_id = self._queue_head.pop()
            except LookupError:
                break

            with suppress(Exception):
                await self.delete_request_lock(request_id)
                # If this fails, we don't have the lock, or the request was never locked. Either way it's fine
