from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING
from typing import OrderedDict as OrderedDictType

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.lru_cache import LRUCache
from crawlee._utils.requests import unique_key_to_request_id
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.models import (
    BaseRequestData,
    ProcessedRequest,
    Request,
    RequestQueueHeadState,
    RequestQueueMetadata,
)
from crawlee.storages.base_storage import BaseStorage
from crawlee.storages.request_provider import RequestProvider

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.base_storage_client import BaseStorageClient
    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class RequestQueue(BaseStorage, RequestProvider):
    """Represents a queue storage for HTTP requests to crawl.

    Manages a queue of requests with unique URLs for structured deep web crawling with support for both breadth-first
    and depth-first orders. This queue is designed for crawling websites by starting with initial URLs and recursively
    following links. Each URL is uniquely identified by a `unique_key` field, which can be overridden to add the same
    URL multiple times under different keys.

    Local storage path (if `CRAWLEE_LOCAL_STORAGE_DIR` is set):
    `{CRAWLEE_LOCAL_STORAGE_DIR}/request_queues/{QUEUE_ID}/{REQUEST_ID}.json`, where `{QUEUE_ID}` is the request
    queue's ID (default or specified) and `{REQUEST_ID}` is the request's ID.

    Usage includes creating or opening existing queues by ID or name, with named queues retained indefinitely and
    unnamed queues expiring after 7 days unless specified otherwise. Supports mutable operations—URLs can be added
    and deleted.

    Usage:
        rq = await RequestQueue.open(id='my_rq_id')
    """

    _API_PROCESSED_REQUESTS_DELAY = timedelta(seconds=10)
    """Delay threshold to assume consistency of queue head operations after queue modifications."""

    _MAX_CACHED_REQUESTS = 1_000_000
    """Maximum number of requests that can be cached."""

    _MAX_HEAD_LIMIT = 1000
    """Cap on requests in progress when querying queue head."""

    _MAX_QUERIES_FOR_CONSISTENCY = 6
    """Maximum attempts to fetch a consistent queue head."""

    _QUERY_HEAD_BUFFER = 3
    """Multiplier for determining the number of requests to fetch based on in-progress requests."""

    _QUERY_HEAD_MIN_LENGTH = 100
    """The minimum number of requests fetched when querying the queue head."""

    _RECENTLY_HANDLED_CACHE_SIZE = 1000
    """Cache size for recently handled requests."""

    _STORAGE_CONSISTENCY_DELAY = timedelta(seconds=3)
    """Expected delay for storage to achieve consistency, guiding the timing of subsequent read operations."""

    def __init__(
        self,
        id: str,
        name: str | None,
        configuration: Configuration,
        client: BaseStorageClient,
    ) -> None:
        self._id = id
        self._name = name
        self._configuration = configuration

        # Get resource clients from storage client
        self._resource_client = client.request_queue(self._id)
        self._resource_collection_client = client.request_queues()

        # Other internal attributes
        self._tasks = list[asyncio.Task]()
        self._client_key = crypto_random_object_id()
        self._internal_timeout_seconds = 5 * 60
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._queue_head_dict: OrderedDictType[str, str] = OrderedDict()
        self._query_queue_head_task: asyncio.Task | None = None
        self._in_progress: set[str] = set()
        self._last_activity = datetime.now(timezone.utc)
        self._recently_handled: LRUCache[bool] = LRUCache(max_length=self._RECENTLY_HANDLED_CACHE_SIZE)
        self._requests_cache: LRUCache[dict] = LRUCache(max_length=self._MAX_CACHED_REQUESTS)

    @override
    @property
    def id(self) -> str:
        return self._id

    @override
    @property
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
    ) -> RequestQueue:
        from crawlee.storages._creation_management import open_storage

        return await open_storage(
            storage_class=cls,
            id=id,
            name=name,
            configuration=configuration,
        )

    @override
    async def drop(self, *, timeout: timedelta | None = None) -> None:
        from crawlee.storages._creation_management import remove_storage_from_cache

        # Wait for all tasks to finish
        await wait_for_all_tasks_for_finish(self._tasks, logger=logger, timeout=timeout)

        # Delete the storage from the underlying client and remove it from the cache
        await self._resource_client.delete()
        remove_storage_from_cache(storage_class=self.__class__, id=self._id, name=self._name)

    async def add_request(
        self,
        request: Request | BaseRequestData | str,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        """Adds a request to the `RequestQueue` while managing deduplication and positioning within the queue.

        The deduplication of requests relies on the `uniqueKey` field within the request dictionary. If `uniqueKey`
        exists, it remains unchanged; if it does not, it is generated based on the request's `url`, `method`,
        and `payload` fields. The generation of `uniqueKey` can be influenced by the `keep_url_fragment` and
        `use_extended_unique_key` flags, which dictate whether to include the URL fragment and the request's method
        and payload, respectively, in its computation.

        The request can be added to the forefront (beginning) or the back of the queue based on the `forefront`
        parameter. Information about the request's addition to the queue, including whether it was already present or
        handled, is returned in an output dictionary.

        Args:
            request: The request object to be added to the queue. Must include at least the `url` key.
                Optionaly it can include the `method`, `payload` and `uniqueKey` keys.

            forefront: If True, adds the request to the forefront of the queue; otherwise, adds it to the end.

            keep_url_fragment: Determines whether the URL fragment (the part of the URL after '#') should be retained
                in the unique key computation.

            use_extended_unique_key: Determines whether to use an extended unique key, incorporating the request's
                method and payload into the unique key computation.

        Returns: A dictionary containing information about the operation, including:
            - `requestId` The ID of the request.
            - `uniqueKey` The unique key associated with the request.
            - `wasAlreadyPresent` (bool): Indicates whether the request was already in the queue.
            - `wasAlreadyHandled` (bool): Indicates whether the request was already processed.
        """
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
            and self._recently_handled.get(request_id) is None
        ):
            self._assumed_total_count += 1
            self._maybe_add_request_to_queue_head(request_id, forefront=forefront)

        return processed_request

    @override
    async def add_requests_batched(
        self,
        requests: Sequence[str | BaseRequestData | Request],
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
        remaining_batches_task = asyncio.create_task(_process_remaining_batches())
        self._tasks.append(remaining_batches_task)

        # Wait for all tasks to finish if requested
        if wait_for_all_requests_to_be_added:
            await wait_for_all_tasks_for_finish(
                self._tasks,
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
        await self.ensure_head_is_non_empty()

        # We are likely done at this point.
        if len(self._queue_head_dict) == 0:
            return None

        next_request_id, _ = self._queue_head_dict.popitem(last=False)  # ~removeFirst()

        # This should never happen, but...
        if next_request_id in self._in_progress or self._recently_handled.get(next_request_id):
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
        self._last_activity = datetime.now(timezone.utc)

        try:
            request = await self.get_request(next_request_id)
        except Exception:
            # On error, remove the request from in progress, otherwise it would be there forever
            self._in_progress.remove(next_request_id)
            raise

        # NOTE: It can happen that the queue head index is inconsistent with the main queue table.
        # This can occur in two situations:

        # 1)
        # Queue head index is ahead of the main table and the request is not present in the main table yet
        # (i.e. getRequest() returned null). In this case, keep the request marked as in progress for a short while,
        # so that isFinished() doesn't return true and _ensureHeadIsNonEmpty() doesn't not load the request into
        # the queueHeadDict straight again. After the interval expires, fetchNextRequest() will try to fetch this
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
        # client, since we keep the track of handled requests in recentlyHandled dictionary). We just add the request
        # to the recentlyHandled dictionary so that next call to _ensureHeadIsNonEmpty() will not put the request again
        # to queueHeadDict.
        if request.handled_at is not None:
            logger.debug(
                'Request fetched from the beginning of queue was already handled',
                extra={'nextRequestId': next_request_id},
            )
            self._recently_handled[next_request_id] = True
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
        self._recently_handled[request.id] = True

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

        The request will be returned for processing later again by another call to `RequestQueue.fetchNextRequest`.

        Args:
            request: The request to return to the queue.
            forefront: Whether to add the request to the head or the end of the queue

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        self._last_activity = datetime.now(timezone.utc)

        if request.id not in self._in_progress:
            logger.debug(f'Cannot reclaim request (ID: {request.id}), because it is not in progress!')
            return None

        # TODO: If request hasn't been changed since the last getRequest(), we don't need to call updateRequest()
        # and thus improve performance.
        # https://github.com/apify/apify-sdk-python/issues/143
        processed_request = await self._resource_client.update_request(request, forefront=forefront)
        processed_request.unique_key = request.unique_key
        self._cache_request(unique_key_to_request_id(request.unique_key), processed_request)

        # Wait a little to increase a chance that the next call to fetchNextRequest() will return the request with
        # updated data. This is to compensate for the limitation of DynamoDB, where writes might not be immediately
        # visible to subsequent reads.
        def callback() -> None:
            if request.id not in self._in_progress:
                logger.debug(f'The request (ID: {request.id}) is no longer marked as in progress in the queue?!')
                return

            self._in_progress.remove(request.id)
            # Performance optimization: add request straight to head if possible
            self._maybe_add_request_to_queue_head(request.id, forefront=forefront)

        asyncio.get_running_loop().call_later(self._STORAGE_CONSISTENCY_DELAY.total_seconds(), callback)
        return processed_request

    async def is_empty(self) -> bool:
        """Check whether the queue is empty.

        Returns:
            bool: `True` if the next call to `RequestQueue.fetchNextRequest` would return `None`, otherwise `False`.
        """
        await self.ensure_head_is_non_empty()
        return len(self._queue_head_dict) == 0

    async def is_finished(self) -> bool:
        """Check whether the queue is finished.

        Due to the nature of distributed storage used by the queue,
        the function might occasionally return a false negative,
        but it will never return a false positive.

        Returns:
            bool: `True` if all requests were already handled and there are no more left. `False` otherwise.
        """
        seconds_since_last_activity = (datetime.now(timezone.utc) - self._last_activity).total_seconds()
        if self._in_progress_count() > 0 and seconds_since_last_activity > self._internal_timeout_seconds:
            message = (
                f'The request queue seems to be stuck for {self._internal_timeout_seconds}s, resetting internal state.'
            )
            logger.warning(message)
            self._reset()

        if len(self._queue_head_dict) > 0 or self._in_progress_count() > 0:
            return False

        # TODO: set ensure_consistency to True once the following issue is resolved:
        # https://github.com/apify/crawlee-python/issues/203
        is_head_consistent = await self.ensure_head_is_non_empty(ensure_consistency=False)
        return is_head_consistent and len(self._queue_head_dict) == 0 and self._in_progress_count() == 0

    async def get_info(self) -> RequestQueueMetadata | None:
        """Get an object containing general information about the request queue."""
        return await self._resource_client.get()

    @override
    async def get_handled_count(self) -> int:
        return self._assumed_handled_count

    @override
    async def get_total_count(self) -> int:
        return self._assumed_total_count

    async def ensure_head_is_non_empty(
        self,
        *,
        ensure_consistency: bool = False,
        limit: int | None = None,
        iteration: int = 0,
    ) -> bool:
        """Ensure that the queue head is non-empty.

        The method ensures that the queue head contains items. It may request more items than are currently
        in progress to guarantee that at least one item is present in the head of the queue.

        Args:
            ensure_consistency: If True, the query for the queue head is retried until the queue_modified_at is older
                than query_started_at by at least API_PROCESSED_REQUESTS_DELAY to ensure that the queue head is
                consistent.
            limit: The maximum number of items to fetch from the queue.
            iteration: To manage the recursion depth.

        Returns:
            True if the queue head is non-empty and consistent, False otherwise.
        """
        # If queue head is non-empty, returns True immediately
        if len(self._queue_head_dict) > 0:
            return True

        if limit is None:
            limit = max(self._in_progress_count() * self._QUERY_HEAD_BUFFER, self._QUERY_HEAD_MIN_LENGTH)

        if self._query_queue_head_task is None:
            self._query_queue_head_task = asyncio.Task(self._queue_query_head(limit))

        queue_head: RequestQueueHeadState = await self._query_queue_head_task

        # TODO: I feel this code below can be greatly simplified... (comes from TS implementation *wink*)
        # https://github.com/apify/apify-sdk-python/issues/142

        # If queue is still empty then one of the following holds:
        # - the other calls waiting for this task already consumed all the returned requests
        # - the limit was too low and contained only requests in progress
        # - the writes from other clients were not propagated yet
        # - the whole queue was processed and we are done

        # If limit was not reached in the call then there are no more requests to be returned.
        if queue_head.prev_limit >= self._MAX_HEAD_LIMIT:
            logger.warning(f'Reached the maximum number of requests in progress (limit: {self._MAX_HEAD_LIMIT})')

        should_repeat_with_higher_limit = (
            len(self._queue_head_dict) == 0
            and queue_head.was_limit_reached
            and queue_head.prev_limit < self._MAX_HEAD_LIMIT
        )

        # If ensure_consistency is True, we must ensure the database is consistent. It can be ensured if either:
        # - queue_modified_at is older than query_started_at by at least _API_PROCESSED_REQUESTS_DELAY
        # - had_multiple_clients is False and _assumed_total_count is less than _assumed_handled_count
        queue_latency = queue_head.query_started_at - queue_head.queue_modified_at.replace(tzinfo=timezone.utc)
        is_database_consistent = queue_latency.total_seconds() >= self._API_PROCESSED_REQUESTS_DELAY.total_seconds()

        is_locally_consistent = (
            not queue_head.had_multiple_clients and self._assumed_total_count <= self._assumed_handled_count
        )

        # Consistent information from one source is enough to consider request queue finished.
        should_repeat_for_consistency = ensure_consistency and not is_database_consistent and not is_locally_consistent

        # If both are false then head is consistent and we may exit.
        if not should_repeat_with_higher_limit and not should_repeat_for_consistency:
            return True

        # If we are querying for consistency then we limit the number of queries to MAX_QUERIES_FOR_CONSISTENCY.
        # If this is reached then we return false so that empty() and finished() returns possibly false negative.
        if not should_repeat_with_higher_limit and iteration > self._MAX_QUERIES_FOR_CONSISTENCY:
            return False

        next_limit = round(queue_head.prev_limit * 1.5) if should_repeat_with_higher_limit else queue_head.prev_limit

        # If we are repeating for consistency then wait required time.
        if should_repeat_for_consistency:
            elapsed_time = (datetime.now(timezone.utc) - queue_head.queue_modified_at).total_seconds()
            delay_seconds = self._API_PROCESSED_REQUESTS_DELAY.total_seconds() - elapsed_time
            logger.info(f'Waiting for {delay_seconds} for queue finalization, to ensure data consistency.')
            await asyncio.sleep(delay_seconds)

        return await self.ensure_head_is_non_empty(
            ensure_consistency=ensure_consistency,
            limit=next_limit,
            iteration=iteration + 1,
        )

    def _in_progress_count(self) -> int:
        return len(self._in_progress)

    def _reset(self) -> None:
        self._queue_head_dict.clear()
        self._query_queue_head_task = None
        self._in_progress.clear()
        self._recently_handled.clear()
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._requests_cache.clear()
        self._last_activity = datetime.now(timezone.utc)

    def _cache_request(self, cache_key: str, processed_request: ProcessedRequest) -> None:
        self._requests_cache[cache_key] = {
            'id': processed_request.id,
            'is_handled': processed_request.was_already_handled,
            'unique_key': processed_request.unique_key,
            'was_already_handled': processed_request.was_already_handled,
        }

    async def _queue_query_head(self, limit: int) -> RequestQueueHeadState:
        query_started_at = datetime.now(timezone.utc)

        list_head = await self._resource_client.list_head(limit=limit)
        list_head_items: list[Request] = list_head.items

        for request in list_head_items:
            # Queue head index might be behind the main table, so ensure we don't recycle requests
            if (
                not request.id
                or not request.unique_key
                or request.id in self._in_progress
                or self._recently_handled.get(request.id)
            ):
                continue

            self._queue_head_dict[request.id] = request.id
            self._cache_request(
                cache_key=unique_key_to_request_id(request.unique_key),
                processed_request=ProcessedRequest(
                    id=request.id,
                    unique_key=request.unique_key,
                    was_already_handled=False,
                    was_already_present=True,
                ),
            )

        # This is needed so that the next call to _ensureHeadIsNonEmpty() will fetch the queue head again.
        self._query_queue_head_task = None

        return RequestQueueHeadState(
            was_limit_reached=len(list_head.items) >= limit,
            prev_limit=limit,
            queue_modified_at=list_head.queue_modified_at,
            query_started_at=query_started_at,
            had_multiple_clients=list_head.had_multiple_clients,
        )

    def _maybe_add_request_to_queue_head(
        self,
        request_id: str,
        *,
        forefront: bool,
    ) -> None:
        if forefront:
            self._queue_head_dict[request_id] = request_id
            # Move to start, i.e. forefront of the queue
            self._queue_head_dict.move_to_end(request_id, last=False)
        elif self._assumed_total_count < self._QUERY_HEAD_MIN_LENGTH:
            # OrderedDict puts the item to the end of the queue by default
            self._queue_head_dict[request_id] = request_id
