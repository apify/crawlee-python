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
from crawlee.request import Request
from crawlee.storages.base_storage import BaseStorage
from crawlee.storages.models import RequestQueueHeadState, RequestQueueOperationInfo
from crawlee.storages.request_provider import RequestProvider

if TYPE_CHECKING:
    from crawlee.configuration import Configuration
    from crawlee.request import BaseRequestData
    from crawlee.resource_clients import RequestQueueClient, RequestQueueCollectionClient
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storages.models import BaseStorageMetadata

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
        client: MemoryStorageClient,
    ) -> None:
        super().__init__(id=id, name=name, client=client, configuration=configuration)

        self._client_key = crypto_random_object_id()
        self._internal_timeout_seconds = 5 * 60
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._request_queue_client = client.request_queue(self.id)
        self._queue_head_dict: OrderedDictType[str, str] = OrderedDict()
        self._query_queue_head_task: asyncio.Task | None = None
        self._in_progress: set[str] = set()
        self._last_activity = datetime.now(timezone.utc)
        self._recently_handled: LRUCache[bool] = LRUCache(max_length=self._RECENTLY_HANDLED_CACHE_SIZE)
        self._requests_cache: LRUCache[dict] = LRUCache(max_length=self._MAX_CACHED_REQUESTS)

    @classmethod
    @override
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> RequestQueue:
        rq = await super().open(id=id, name=name, configuration=configuration)
        await rq.ensure_head_is_non_empty()
        return rq

    @classmethod
    @override
    def _get_human_friendly_label(cls) -> str:
        return 'Request queue'

    @classmethod
    @override
    def _get_default_id(cls, configuration: Configuration) -> str:
        return configuration.default_request_queue_id

    @classmethod
    @override
    def _get_single_storage_client(
        cls,
        id: str,
        client: MemoryStorageClient,
    ) -> RequestQueueClient:
        return client.request_queue(id)

    @classmethod
    @override
    def _get_storage_collection_client(
        cls,
        client: MemoryStorageClient,
    ) -> RequestQueueCollectionClient:
        return client.request_queues()

    async def add_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> RequestQueueOperationInfo:
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
        self._last_activity = datetime.now(timezone.utc)

        cache_key = unique_key_to_request_id(request.unique_key)
        cached_info = self._requests_cache.get(cache_key)

        if cached_info:
            request.id = cached_info['id']
            # We may assume that if request is in local cache then also the information if the request was already
            # handled is there because just one client should be using one queue.
            return RequestQueueOperationInfo(
                request_id=request.id,
                request_unique_key=request.unique_key,
                was_already_present=True,
                was_already_handled=cached_info['wasAlreadyHandled'],
            )

        queue_operation_info = await self._request_queue_client.add_request(request, forefront=forefront)
        queue_operation_info.request_unique_key = request.unique_key

        self._cache_request(cache_key, queue_operation_info)

        request_id, was_already_present = queue_operation_info.request_id, queue_operation_info.was_already_present
        is_handled = request.handled_at is not None

        if (
            not is_handled
            and not was_already_present
            and request_id not in self._in_progress
            and self._recently_handled.get(request_id) is None
        ):
            self._assumed_total_count += 1
            self._maybe_add_request_to_queue_head(request_id, forefront=forefront)

        return queue_operation_info

    @override
    async def add_requests_batched(
        self,
        requests: list[BaseRequestData | Request],
        *,
        batch_size: int,
        wait_for_all_requests_to_be_added: bool,
        wait_time_between_batches: timedelta,
    ) -> None:
        for request in requests:
            if isinstance(request, Request):
                await self.add_request(request)
            else:
                await self.add_request(Request.from_base_request_data(request))

    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve.

        Returns:
            The retrieved request, or `None`, if it does not exist.
        """
        return await self._request_queue_client.get_request(request_id)

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

    async def mark_request_as_handled(self, request: Request) -> RequestQueueOperationInfo | None:
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

        queue_operation_info = await self._request_queue_client.update_request(request)
        queue_operation_info.request_unique_key = request.unique_key

        self._in_progress.remove(request.id)
        self._recently_handled[request.id] = True

        if not queue_operation_info.was_already_handled:
            self._assumed_handled_count += 1

        self._cache_request(unique_key_to_request_id(request.unique_key), queue_operation_info)
        return queue_operation_info

    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> RequestQueueOperationInfo | None:
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
        queue_operation_info = await self._request_queue_client.update_request(request, forefront=forefront)
        queue_operation_info.request_unique_key = request.unique_key
        self._cache_request(unique_key_to_request_id(request.unique_key), queue_operation_info)

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
        return queue_operation_info

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

        is_head_consistent = await self.ensure_head_is_non_empty(ensure_consistency=True)
        return is_head_consistent and len(self._queue_head_dict) == 0 and self._in_progress_count() == 0

    async def drop(self) -> None:
        """Remove the request queue either from the Apify cloud storage or from the local directory."""
        await self._request_queue_client.delete()
        self._remove_from_cache()

    async def get_info(self) -> BaseStorageMetadata | None:
        """Get an object containing general information about the request queue.

        Returns:
            Object returned by calling the GET request queue API endpoint.
        """
        return await self._request_queue_client.get()

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
        """Ensure that the queue head is nonempty."""
        # If is nonempty resolve immediately.
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

        # If ensureConsistency=true then we must ensure that either:
        # - queueModifiedAt is older than queryStartedAt by at least _API_PROCESSED_REQUESTS_DELAY
        # - hadMultipleClients=false and this.assumedTotalCount<=this.assumedHandledCount
        is_database_consistent = (
            queue_head.query_started_at - queue_head.queue_modified_at.replace(tzinfo=timezone.utc)
        ).total_seconds() >= (self._API_PROCESSED_REQUESTS_DELAY.total_seconds())

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

    def _cache_request(self, cache_key: str, operation_info: RequestQueueOperationInfo) -> None:
        self._requests_cache[cache_key] = {
            'id': operation_info.request_id,
            'isHandled': operation_info.was_already_handled,
            'uniqueKey': operation_info.request_unique_key,
            'wasAlreadyHandled': operation_info.was_already_handled,
        }

    async def _queue_query_head(self, limit: int) -> RequestQueueHeadState:
        query_started_at = datetime.now(timezone.utc)

        list_head = await self._request_queue_client.list_head(limit=limit)
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
                operation_info=RequestQueueOperationInfo(
                    request_id=request.id,
                    was_already_handled=False,
                    was_already_present=True,
                    request_unique_key=request.unique_key,
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
