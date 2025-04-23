from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar

from cachetools import LRUCache
from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee._utils.requests import unique_key_to_request_id
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.events import Event
from crawlee.request_loaders import RequestManager
from crawlee.storage_clients.models import ProcessedRequest, RequestQueueMetadata, StorageMetadata

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient

logger = getLogger(__name__)

T = TypeVar('T')


class CachedRequest(TypedDict):
    id: str
    was_already_handled: bool
    hydrated: Request | None
    lock_expires_at: datetime | None
    forefront: bool


@docs_group('Classes')
class RequestQueue(Storage, RequestManager):
    """Represents a queue storage for managing HTTP requests in web crawling operations.

    The `RequestQueue` class handles a queue of HTTP requests, each identified by a unique URL, to facilitate structured
    web crawling. It supports both breadth-first and depth-first crawling strategies, allowing for recursive crawling
    starting from an initial set of URLs. Each URL in the queue is uniquely identified by a `unique_key`, which can be
    customized to allow the same URL to be added multiple times under different keys.

    Data can be stored either locally or in the cloud. It depends on the setup of underlying storage client.
    By default a `MemoryStorageClient` is used, but it can be changed to a different one.

    By default, data is stored using the following path structure:
    ```
    {CRAWLEE_STORAGE_DIR}/request_queues/{QUEUE_ID}/{REQUEST_ID}.json
    ```
    - `{CRAWLEE_STORAGE_DIR}`: The root directory for all storage data specified by the environment variable.
    - `{QUEUE_ID}`: The identifier for the request queue, either "default" or as specified.
    - `{REQUEST_ID}`: The unique identifier for each request in the queue.

    The `RequestQueue` supports both creating new queues and opening existing ones by `id` or `name`. Named queues
    persist indefinitely, while unnamed queues expire after 7 days unless specified otherwise. The queue supports
    mutable operations, allowing URLs to be added and removed as needed.

    ### Usage

    ```python
    from crawlee.storages import RequestQueue

    rq = await RequestQueue.open(name='my_rq')
    ```
    """

    _MAX_CACHED_REQUESTS = 1_000_000
    """Maximum number of requests that can be cached."""

    def __init__(
        self,
        id: str,
        name: str | None,
        storage_client: StorageClient,
    ) -> None:
        config = service_locator.get_configuration()
        event_manager = service_locator.get_event_manager()

        self._id = id
        self._name = name

        datetime_now = datetime.now(timezone.utc)
        self._storage_object = StorageMetadata(
            id=id, name=name, accessed_at=datetime_now, created_at=datetime_now, modified_at=datetime_now
        )

        # Get resource clients from storage client
        self._resource_client = storage_client.request_queue(self._id)
        self._resource_collection_client = storage_client.request_queues()

        self._request_lock_time = timedelta(minutes=3)
        self._queue_paused_for_migration = False
        self._queue_has_locked_requests: bool | None = None
        self._should_check_for_forefront_requests = False

        self._is_finished_log_throttle_counter = 0
        self._dequeued_request_count = 0

        event_manager.on(event=Event.MIGRATING, listener=lambda _: setattr(self, '_queue_paused_for_migration', True))
        event_manager.on(event=Event.MIGRATING, listener=self._clear_possible_locks)
        event_manager.on(event=Event.ABORTING, listener=self._clear_possible_locks)

        # Other internal attributes
        self._tasks = list[asyncio.Task]()
        self._client_key = crypto_random_object_id()
        self._internal_timeout = config.internal_timeout or timedelta(minutes=5)
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._queue_head = deque[str]()
        self._list_head_and_lock_task: asyncio.Task | None = None
        self._last_activity = datetime.now(timezone.utc)
        self._requests_cache: LRUCache[str, CachedRequest] = LRUCache(maxsize=self._MAX_CACHED_REQUESTS)

    @classmethod
    def from_storage_object(cls, storage_client: StorageClient, storage_object: StorageMetadata) -> RequestQueue:
        """Initialize a new instance of RequestQueue from a storage metadata object."""
        request_queue = RequestQueue(
            id=storage_object.id,
            name=storage_object.name,
            storage_client=storage_client,
        )

        request_queue.storage_object = storage_object
        return request_queue

    @property
    @override
    def id(self) -> str:
        return self._id

    @property
    @override
    def name(self) -> str | None:
        return self._name

    @property
    @override
    def storage_object(self) -> StorageMetadata:
        return self._storage_object

    @storage_object.setter
    @override
    def storage_object(self, storage_object: StorageMetadata) -> None:
        self._storage_object = storage_object

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: StorageClient | None = None,
    ) -> RequestQueue:
        from crawlee.storages._creation_management import open_storage

        configuration = configuration or service_locator.get_configuration()
        storage_client = storage_client or service_locator.get_storage_client()

        return await open_storage(
            storage_class=cls,
            id=id,
            name=name,
            configuration=configuration,
            storage_client=storage_client,
        )

    @override
    async def drop(self, *, timeout: timedelta | None = None) -> None:
        from crawlee.storages._creation_management import remove_storage_from_cache

        # Wait for all tasks to finish
        await wait_for_all_tasks_for_finish(self._tasks, logger=logger, timeout=timeout)

        # Delete the storage from the underlying client and remove it from the cache
        await self._resource_client.delete()
        remove_storage_from_cache(storage_class=self.__class__, id=self._id, name=self._name)

    @override
    async def add_request(
        self,
        request: str | Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        request = self._transform_request(request)
        self._last_activity = datetime.now(timezone.utc)

        cache_key = unique_key_to_request_id(request.unique_key)
        cached_info = self._requests_cache.get(cache_key)

        if cached_info:
            request.id = cached_info['id']
            # We may assume that if request is in local cache then also the information if the request was already
            # handled is there because just one client should be using one queue.
            return ProcessedRequest(
                id=request.id,
                unique_key=request.unique_key,
                was_already_present=True,
                was_already_handled=cached_info['was_already_handled'],
            )

        processed_request = await self._resource_client.add_request(request, forefront=forefront)
        processed_request.unique_key = request.unique_key

        self._cache_request(cache_key, processed_request, forefront=forefront)

        if not processed_request.was_already_present and forefront:
            self._should_check_for_forefront_requests = True

        if request.handled_at is None and not processed_request.was_already_present:
            self._assumed_total_count += 1

        return processed_request

    @override
    async def add_requests_batched(
        self,
        requests: Sequence[str | Request],
        *,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        transformed_requests = self._transform_requests(requests)
        wait_time_secs = wait_time_between_batches.total_seconds()

        # Wait for the first batch to be added
        first_batch = transformed_requests[:batch_size]
        if first_batch:
            await self._process_batch(first_batch, base_retry_wait=wait_time_between_batches)

        async def _process_remaining_batches() -> None:
            for i in range(batch_size, len(transformed_requests), batch_size):
                batch = transformed_requests[i : i + batch_size]
                await self._process_batch(batch, base_retry_wait=wait_time_between_batches)
                if i + batch_size < len(transformed_requests):
                    await asyncio.sleep(wait_time_secs)

        # Create and start the task to process remaining batches in the background
        remaining_batches_task = asyncio.create_task(
            _process_remaining_batches(), name='request_queue_process_remaining_batches_task'
        )
        self._tasks.append(remaining_batches_task)
        remaining_batches_task.add_done_callback(lambda _: self._tasks.remove(remaining_batches_task))

        # Wait for all tasks to finish if requested
        if wait_for_all_requests_to_be_added:
            await wait_for_all_tasks_for_finish(
                (remaining_batches_task,),
                logger=logger,
                timeout=wait_for_all_requests_to_be_added_timeout,
            )

    async def _process_batch(self, batch: Sequence[Request], base_retry_wait: timedelta, attempt: int = 1) -> None:
        max_attempts = 5
        response = await self._resource_client.batch_add_requests(batch)

        if response.unprocessed_requests:
            logger.debug(f'Following requests were not processed: {response.unprocessed_requests}.')
            if attempt > max_attempts:
                logger.warning(
                    f'Following requests were not processed even after {max_attempts} attempts:\n'
                    f'{response.unprocessed_requests}'
                )
            else:
                logger.debug('Retry to add requests.')
                unprocessed_requests_unique_keys = {request.unique_key for request in response.unprocessed_requests}
                retry_batch = [request for request in batch if request.unique_key in unprocessed_requests_unique_keys]
                await asyncio.sleep((base_retry_wait * attempt).total_seconds())
                await self._process_batch(retry_batch, base_retry_wait=base_retry_wait, attempt=attempt + 1)

        request_count = len(batch) - len(response.unprocessed_requests)
        self._assumed_total_count += request_count
        if request_count:
            logger.debug(
                f'Added {request_count} requests to the queue. Processed requests: {response.processed_requests}'
            )

    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve.

        Returns:
            The retrieved request, or `None`, if it does not exist.
        """
        return await self._resource_client.get_request(request_id)

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

    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `RequestQueue.fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        self._last_activity = datetime.now(timezone.utc)

        if request.handled_at is None:
            request.handled_at = datetime.now(timezone.utc)

        processed_request = await self._resource_client.update_request(request)
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

        The request will be returned for processing later again by another call to `RequestQueue.fetch_next_request`.

        Args:
            request: The request to return to the queue.
            forefront: Whether to add the request to the head or the end of the queue.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        self._last_activity = datetime.now(timezone.utc)

        processed_request = await self._resource_client.update_request(request, forefront=forefront)
        processed_request.unique_key = request.unique_key
        self._cache_request(unique_key_to_request_id(request.unique_key), processed_request, forefront=forefront)

        if forefront:
            self._should_check_for_forefront_requests = True

        if processed_request:
            # Try to delete the request lock if possible
            try:
                await self._resource_client.delete_request_lock(request.id, forefront=forefront)
            except Exception as err:
                logger.debug(f'Failed to delete request lock for request {request.id}', exc_info=err)

        return processed_request

    async def is_empty(self) -> bool:
        """Check whether the queue is empty.

        Returns:
            bool: `True` if the next call to `RequestQueue.fetch_next_request` would return `None`, otherwise `False`.
        """
        await self._ensure_head_is_non_empty()
        return len(self._queue_head) == 0

    async def is_finished(self) -> bool:
        """Check whether the queue is finished.

        Due to the nature of distributed storage used by the queue, the function might occasionally return a false
        negative, but it will never return a false positive.

        Returns:
            bool: `True` if all requests were already handled and there are no more left. `False` otherwise.
        """
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

        metadata = await self._resource_client.get()
        if metadata is not None and not metadata.had_multiple_clients and not self._queue_head:
            logger.debug('Queue head is empty and there are no other clients - we are finished')

            return True

        # The following is a legacy algorithm for checking if the queue is finished.
        # It is used only for request queue clients that do not provide the `queue_has_locked_requests` flag.
        current_head = await self._resource_client.list_head(limit=2)

        if current_head.items:
            logger.debug('The queue still contains unfinished requests or requests locked by another client')

        return len(current_head.items) == 0

    async def get_info(self) -> RequestQueueMetadata | None:
        """Get an object containing general information about the request queue."""
        return await self._resource_client.get()

    @override
    async def get_handled_count(self) -> int:
        return self._assumed_handled_count

    @override
    async def get_total_count(self) -> int:
        return self._assumed_total_count

    async def _ensure_head_is_non_empty(self) -> None:
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

    async def _list_head_and_lock(self) -> None:
        # Make a copy so that we can clear the flag only if the whole method executes after the flag was set
        # (i.e, it was not set in the middle of the execution of the method)
        should_check_for_forefront_requests = self._should_check_for_forefront_requests

        limit = 25

        response = await self._resource_client.list_and_lock_head(
            limit=limit, lock_secs=int(self._request_lock_time.total_seconds())
        )

        self._queue_has_locked_requests = response.queue_has_locked_requests

        head_id_buffer = list[str]()
        forefront_head_id_buffer = list[str]()

        for request in response.items:
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
                    await self._resource_client.delete_request_lock(request.id)

                continue

            # If we remember that we added the request ourselves and we added it to the forefront,
            # we will put it to the beginning of the local queue head to preserve the expected order.
            # If we do not remember that, we will enqueue it normally.
            cached_request = self._requests_cache.get(unique_key_to_request_id(request.unique_key))
            forefront = cached_request['forefront'] if cached_request else False

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
                *[self._resource_client.delete_request_lock(request_id) for request_id in to_unlock],
                return_exceptions=True,  # Just ignore the exceptions
            )

        # Unset the should_check_for_forefront_requests flag - the check is finished
        if should_check_for_forefront_requests:
            self._should_check_for_forefront_requests = False

    def _reset(self) -> None:
        self._queue_head.clear()
        self._list_head_and_lock_task = None
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._requests_cache.clear()
        self._last_activity = datetime.now(timezone.utc)

    def _cache_request(self, cache_key: str, processed_request: ProcessedRequest, *, forefront: bool) -> None:
        self._requests_cache[cache_key] = {
            'id': processed_request.id,
            'was_already_handled': processed_request.was_already_handled,
            'hydrated': None,
            'lock_expires_at': None,
            'forefront': forefront,
        }

    async def _get_or_hydrate_request(self, request_id: str) -> Request | None:
        cached_entry = self._requests_cache.get(request_id)

        if not cached_entry:
            # 2.1. Attempt to prolong the request lock to see if we still own the request
            prolong_result = await self._prolong_request_lock(request_id)

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
                    await self._resource_client.delete_request_lock(request_id)

                return None

            self._requests_cache[request_id] = {
                'id': request_id,
                'hydrated': hydrated_request,
                'was_already_handled': hydrated_request.handled_at is not None,
                'lock_expires_at': prolong_result,
                'forefront': False,
            }

            return hydrated_request

        # 1.1. If hydrated, prolong the lock more and return it
        if cached_entry['hydrated']:
            # 1.1.1. If the lock expired on the hydrated requests, try to prolong. If we fail, we lost the request
            # (or it was handled already)
            if cached_entry['lock_expires_at'] and cached_entry['lock_expires_at'] < datetime.now(timezone.utc):
                prolonged = await self._prolong_request_lock(cached_entry['id'])

                if not prolonged:
                    return None

                cached_entry['lock_expires_at'] = prolonged

            return cached_entry['hydrated']

        # 1.2. If not hydrated, try to prolong the lock first (to ensure we keep it in our queue), hydrate and return it
        prolonged = await self._prolong_request_lock(cached_entry['id'])

        if not prolonged:
            return None

        # This might still return null if the queue head is inconsistent with the main queue table.
        hydrated_request = await self.get_request(cached_entry['id'])

        cached_entry['hydrated'] = hydrated_request

        # Queue head index is ahead of the main table and the request is not present in the main table yet
        # (i.e. get_request() returned null).
        if not hydrated_request:
            # Remove the lock from the request for now, so that it can be picked up later
            # This may/may not succeed, but that's fine
            with suppress(Exception):
                await self._resource_client.delete_request_lock(cached_entry['id'])

            return None

        return hydrated_request

    async def _prolong_request_lock(self, request_id: str) -> datetime | None:
        try:
            res = await self._resource_client.prolong_request_lock(
                request_id, lock_secs=int(self._request_lock_time.total_seconds())
            )
        except Exception as err:
            # Most likely we do not own the lock anymore
            logger.warning(
                f'Failed to prolong lock for cached request {request_id}, either lost the lock '
                'or the request was already handled\n',
                exc_info=err,
            )
            return None
        else:
            return res.lock_expires_at

    async def _clear_possible_locks(self) -> None:
        self._queue_paused_for_migration = True
        request_id: str | None = None

        while True:
            try:
                request_id = self._queue_head.pop()
            except LookupError:
                break

            with suppress(Exception):
                await self._resource_client.delete_request_lock(request_id)
                # If this fails, we don't have the lock, or the request was never locked. Either way it's fine
