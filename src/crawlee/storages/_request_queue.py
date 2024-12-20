from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generic, TypedDict, TypeVar

from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee._utils.lru_cache import LRUCache
from crawlee._utils.requests import unique_key_to_request_id
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.events import Event
from crawlee.request_loaders import RequestManager
from crawlee.storage_clients.models import ProcessedRequest, RequestQueueMetadata

from ._base_storage import BaseStorage

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import BaseStorageClient

logger = getLogger(__name__)

T = TypeVar('T')


class BoundedSet(Generic[T]):
    """A simple set datastructure that removes the least recently accessed item when it reaches `max_length`."""

    def __init__(self, max_length: int) -> None:
        self._max_length = max_length
        self._data = OrderedDict[T, object]()

    def __contains__(self, item: T) -> bool:
        found = item in self._data
        if found:
            self._data.move_to_end(item, last=True)
        return found

    def add(self, item: T) -> None:
        self._data[item] = True
        self._data.move_to_end(item)

        if len(self._data) > self._max_length:
            self._data.popitem(last=False)

    def clear(self) -> None:
        self._data.clear()


class CachedRequest(TypedDict):
    id: str
    was_already_handled: bool
    hydrated: Request | None
    lock_expires_at: datetime | None


@docs_group('Classes')
class RequestQueue(BaseStorage, RequestManager):
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

    _RECENTLY_HANDLED_CACHE_SIZE = 1000
    """Cache size for recently handled requests."""

    _STORAGE_CONSISTENCY_DELAY = timedelta(seconds=3)
    """Expected delay for storage to achieve consistency, guiding the timing of subsequent read operations."""

    def __init__(self, id: str, name: str | None, storage_client: BaseStorageClient) -> None:
        config = service_locator.get_configuration()
        event_manager = service_locator.get_event_manager()

        self._id = id
        self._name = name

        # Get resource clients from storage client
        self._resource_client = storage_client.request_queue(self._id)
        self._resource_collection_client = storage_client.request_queues()

        self._request_lock_time = timedelta(minutes=3)
        self._queue_paused_for_migration = False

        event_manager.on(event=Event.MIGRATING, listener=lambda _: setattr(self, '_queue_paused_for_migration', True))
        event_manager.on(event=Event.MIGRATING, listener=self._clear_possible_locks)
        event_manager.on(event=Event.ABORTING, listener=self._clear_possible_locks)

        # Other internal attributes
        self._tasks = list[asyncio.Task]()
        self._client_key = crypto_random_object_id()
        self._internal_timeout = config.internal_timeout or timedelta(minutes=5)
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._queue_head_dict: OrderedDict[str, str] = OrderedDict()
        self._list_head_and_lock_task: asyncio.Task | None = None
        self._in_progress: set[str] = set()
        self._last_activity = datetime.now(timezone.utc)
        self._recently_handled: BoundedSet[str] = BoundedSet(max_length=self._RECENTLY_HANDLED_CACHE_SIZE)
        self._requests_cache: LRUCache[CachedRequest] = LRUCache(max_length=self._MAX_CACHED_REQUESTS)

    @property
    @override
    def id(self) -> str:
        return self._id

    @property
    @override
    def name(self) -> str | None:
        return self._name

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: BaseStorageClient | None = None,
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

        self._cache_request(cache_key, processed_request)

        request_id, was_already_present = processed_request.id, processed_request.was_already_present
        is_handled = request.handled_at is not None

        if (
            not is_handled
            and not was_already_present
            and request_id not in self._in_progress
            and request_id not in self._recently_handled
        ):
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

        async def _process_batch(batch: Sequence[Request]) -> None:
            request_count = len(batch)
            response = await self._resource_client.batch_add_requests(batch)
            self._assumed_total_count += request_count
            logger.debug(f'Added {request_count} requests to the queue, response: {response}')

        # Wait for the first batch to be added
        first_batch = transformed_requests[:batch_size]
        if first_batch:
            await _process_batch(first_batch)

        async def _process_remaining_batches() -> None:
            for i in range(batch_size, len(transformed_requests), batch_size):
                batch = transformed_requests[i : i + batch_size]
                await _process_batch(batch)
                if i + batch_size < len(transformed_requests):
                    await asyncio.sleep(wait_time_secs)

        # Create and start the task to process remaining batches in the background
        remaining_batches_task = asyncio.create_task(
            _process_remaining_batches(), name='request_queue_process_remaining_batches_task'
        )
        self._tasks.append(remaining_batches_task)

        # Wait for all tasks to finish if requested
        if wait_for_all_requests_to_be_added:
            await wait_for_all_tasks_for_finish(
                (remaining_batches_task,),
                logger=logger,
                timeout=wait_for_all_requests_to_be_added_timeout,
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
        if len(self._queue_head_dict) == 0:
            return None

        next_request_id, _ = self._queue_head_dict.popitem(last=False)  # ~removeFirst()

        # This should never happen, but...
        if next_request_id in self._in_progress or next_request_id in self._recently_handled:
            logger.warning(
                'Queue head returned a request that is already in progress?!',
                extra={
                    'nextRequestId': next_request_id,
                    'inProgress': next_request_id in self._in_progress,
                    'recentlyHandled': next_request_id in self._recently_handled,
                },
            )
            return None

        self._in_progress.add(next_request_id)

        try:
            request = await self._get_or_hydrate_request(next_request_id)
        except Exception:
            # On error, remove the request from in progress, otherwise it would be there forever
            self._in_progress.remove(next_request_id)
            raise

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
            asyncio.get_running_loop().call_later(
                self._STORAGE_CONSISTENCY_DELAY.total_seconds(),
                lambda: self._in_progress.remove(next_request_id),
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
            self._recently_handled.add(next_request_id)
            return None

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

        if request.id not in self._in_progress:
            logger.debug(f'Cannot mark request (ID: {request.id}) as handled, because it is not in progress!')
            return None

        if request.handled_at is None:
            request.handled_at = datetime.now(timezone.utc)

        processed_request = await self._resource_client.update_request(request)
        processed_request.unique_key = request.unique_key

        self._in_progress.remove(request.id)
        self._recently_handled.add(request.id)

        if not processed_request.was_already_handled:
            self._assumed_handled_count += 1

        self._cache_request(unique_key_to_request_id(request.unique_key), processed_request)
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

        if request.id not in self._in_progress:
            logger.debug(f'Cannot reclaim request (ID: {request.id}), because it is not in progress!')
            return None

        # TODO: If request hasn't been changed since the last get_request(), we don't need to call update_request()
        # and thus improve performance.
        # https://github.com/apify/apify-sdk-python/issues/143
        processed_request = await self._resource_client.update_request(request, forefront=forefront)
        processed_request.unique_key = request.unique_key
        self._cache_request(unique_key_to_request_id(request.unique_key), processed_request)

        if processed_request:
            # Mark the request as no longer in progress,
            # as the moment we delete the lock, we could end up also re-fetching the request in a subsequent
            # _ensure_head_is_non_empty() which could potentially lock the request again
            self._in_progress.discard(request.id)

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
        return len(self._queue_head_dict) == 0

    async def is_finished(self) -> bool:
        """Check whether the queue is finished.

        Due to the nature of distributed storage used by the queue, the function might occasionally return a false
        negative, but it will never return a false positive.

        Returns:
            bool: `True` if all requests were already handled and there are no more left. `False` otherwise.
        """
        seconds_since_last_activity = datetime.now(timezone.utc) - self._last_activity
        if self._in_progress_count() > 0 and seconds_since_last_activity > self._internal_timeout:
            logger.warning(
                f'The request queue seems to be stuck for {self._internal_timeout.total_seconds()}s, '
                'resetting internal state.',
                extra={
                    'queue_head_ids_pending': len(self._queue_head_dict),
                    'in_progress': list(self._in_progress),
                },
            )

            # We only need to reset these two variables, no need to reset all the other stats
            self._queue_head_dict.clear()
            self._in_progress.clear()

        if self._queue_head_dict:
            logger.debug(
                'There are still ids in the queue head that are pending processing',
                extra={
                    'queue_head_ids_pending': len(self._queue_head_dict),
                },
            )

            return False

        if self._in_progress:
            logger.debug(
                'There are still requests in progress (or zombie)',
                extra={
                    'in_progress': list(self._in_progress),
                },
            )

            return False

        current_head = await self._resource_client.list_head(limit=2)

        if current_head.items:
            logger.debug(
                'Queue head still returned requests that need to be processed (or that are locked by other clients)',
            )

        return (not current_head.items and not self._in_progress) or self._queue_paused_for_migration

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
        if len(self._queue_head_dict) > 1:
            return

        if self._list_head_and_lock_task is None:
            task = asyncio.create_task(self._list_head_and_lock(), name='request_queue_list_head_and_lock_task')

            def callback(_: Any) -> None:
                self._list_head_and_lock_task = None

            task.add_done_callback(callback)
            self._list_head_and_lock_task = task

        await self._list_head_and_lock_task

    async def _list_head_and_lock(self) -> None:
        response = await self._resource_client.list_and_lock_head(
            limit=25, lock_secs=int(self._request_lock_time.total_seconds())
        )

        for request in response.items:
            # Queue head index might be behind the main table, so ensure we don't recycle requests
            if (
                not request.id
                or not request.unique_key
                or request.id in self._in_progress
                or request.id in self._recently_handled
            ):
                logger.debug(
                    'Skipping request from queue head, already in progress or recently handled',
                    extra={
                        'id': request.id,
                        'unique_key': request.unique_key,
                        'in_progress': request.id in self._in_progress,
                        'recently_handled': request.id in self._recently_handled,
                    },
                )

                # Remove the lock from the request for now, so that it can be picked up later
                # This may/may not succeed, but that's fine
                with suppress(Exception):
                    await self._resource_client.delete_request_lock(request.id)

                continue

            self._queue_head_dict[request.id] = request.id
            self._cache_request(
                unique_key_to_request_id(request.unique_key),
                ProcessedRequest(
                    id=request.id,
                    unique_key=request.unique_key,
                    was_already_present=True,
                    was_already_handled=False,
                ),
            )

    def _in_progress_count(self) -> int:
        return len(self._in_progress)

    def _reset(self) -> None:
        self._queue_head_dict.clear()
        self._list_head_and_lock_task = None
        self._in_progress.clear()
        self._recently_handled.clear()
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._requests_cache.clear()
        self._last_activity = datetime.now(timezone.utc)

    def _cache_request(self, cache_key: str, processed_request: ProcessedRequest) -> None:
        self._requests_cache[cache_key] = {
            'id': processed_request.id,
            'was_already_handled': processed_request.was_already_handled,
            'hydrated': None,
            'lock_expires_at': None,
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
                request_id, _ = self._queue_head_dict.popitem()
            except KeyError:
                break

            with suppress(Exception):
                await self._resource_client.delete_request_lock(request_id)
                # If this fails, we don't have the lock, or the request was never locked. Either way it's fine
