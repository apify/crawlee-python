from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from typing import OrderedDict as OrderedDictType

from apify_shared.utils import ignore_docs

from apify._crypto import crypto_random_object_id
from apify._utils import LRUCache, budget_ow, compute_unique_key, unique_key_to_request_id
from apify.consts import REQUEST_QUEUE_HEAD_MAX_LIMIT
from apify.log import logger
from apify.storages.base_storage import BaseStorage

if TYPE_CHECKING:
    from apify_client import ApifyClientAsync
    from apify_client.clients import RequestQueueClientAsync, RequestQueueCollectionClientAsync

    from apify._memory_storage import MemoryStorageClient
    from apify._memory_storage.resource_clients import RequestQueueClient, RequestQueueCollectionClient
    from apify.config import Configuration


MAX_CACHED_REQUESTS = 1_000_000

# When requesting queue head we always fetch requestsInProgressCount * QUERY_HEAD_BUFFER number of requests.
QUERY_HEAD_MIN_LENGTH = 100

QUERY_HEAD_BUFFER = 3

# If queue was modified (request added/updated/deleted) before more than API_PROCESSED_REQUESTS_DELAY_MILLIS
# then we assume the get head operation to be consistent.
API_PROCESSED_REQUESTS_DELAY_MILLIS = 10_000

# How many times we try to get queue head with queueModifiedAt older than API_PROCESSED_REQUESTS_DELAY_MILLIS.
MAX_QUERIES_FOR_CONSISTENCY = 6

# This number must be large enough so that processing of all these requests cannot be done in
# a time lower than expected maximum latency of DynamoDB, but low enough not to waste too much memory.
RECENTLY_HANDLED_CACHE_SIZE = 1000

# Indicates how long it usually takes for the underlying storage to propagate all writes
# to be available to subsequent reads.
STORAGE_CONSISTENCY_DELAY_MILLIS = 3000


class RequestQueue(BaseStorage):
    """Represents a queue of URLs to crawl.

    Can be used for deep crawling of websites where you start with several URLs and then recursively
    follow links to other pages. The data structure supports both breadth-first and depth-first crawling orders.

    Each URL is represented using an instance of the {@apilink Request} class.
    The queue can only contain unique URLs. More precisely, it can only contain request dictionaries
    with distinct `uniqueKey` properties. By default, `uniqueKey` is generated from the URL, but it can also be overridden.
    To add a single URL multiple times to the queue,
    corresponding request dictionary will need to have different `uniqueKey` properties.

    Do not instantiate this class directly, use the `Actor.open_request_queue()` function instead.

    `RequestQueue` stores its data either on local disk or in the Apify cloud,
    depending on whether the `APIFY_LOCAL_STORAGE_DIR` or `APIFY_TOKEN` environment variables are set.

    If the `APIFY_LOCAL_STORAGE_DIR` environment variable is set, the data is stored in
    the local directory in the following files:
    ```
    {APIFY_LOCAL_STORAGE_DIR}/request_queues/{QUEUE_ID}/{REQUEST_ID}.json
    ```
    Note that `{QUEUE_ID}` is the name or ID of the request queue. The default request queue has ID: `default`,
    unless you override it by setting the `APIFY_DEFAULT_REQUEST_QUEUE_ID` environment variable.
    The `{REQUEST_ID}` is the id of the request.

    If the `APIFY_TOKEN` environment variable is set but `APIFY_LOCAL_STORAGE_DIR` is not, the data is stored in the
    [Apify Request Queue](https://docs.apify.com/storage/request-queue)
    cloud storage.
    """

    _request_queue_client: RequestQueueClientAsync | RequestQueueClient
    _client_key = crypto_random_object_id()
    _queue_head_dict: OrderedDictType[str, str]
    _query_queue_head_task: asyncio.Task | None
    _in_progress: set[str]
    _last_activity: datetime
    _internal_timeout_seconds = 5 * 60
    _recently_handled: LRUCache[bool]
    _assumed_total_count = 0
    _assumed_handled_count = 0
    _requests_cache: LRUCache[dict]

    @ignore_docs
    def __init__(
        self: RequestQueue,
        id: str,  # noqa: A002
        name: str | None,
        client: ApifyClientAsync | MemoryStorageClient,
        config: Configuration,
    ) -> None:
        """Create a `RequestQueue` instance.

        Do not use the constructor directly, use the `Actor.open_request_queue()` function instead.

        Args:
            id (str): ID of the request queue.
            name (str, optional): Name of the request queue.
            client (ApifyClientAsync or MemoryStorageClient): The storage client which should be used.
            config (Configuration): The configuration which should be used.
        """
        super().__init__(id=id, name=name, client=client, config=config)

        self._request_queue_client = client.request_queue(self._id, client_key=self._client_key)
        self._queue_head_dict = OrderedDict()
        self._query_queue_head_task = None
        self._in_progress = set()
        self._last_activity = datetime.now(timezone.utc)
        self._recently_handled = LRUCache[bool](max_length=RECENTLY_HANDLED_CACHE_SIZE)
        self._requests_cache = LRUCache(max_length=MAX_CACHED_REQUESTS)

    @classmethod
    def _get_human_friendly_label(cls: type[RequestQueue]) -> str:
        return 'Request queue'

    @classmethod
    def _get_default_id(cls: type[RequestQueue], config: Configuration) -> str:
        return config.default_request_queue_id

    @classmethod
    def _get_single_storage_client(
        cls: type[RequestQueue],
        id: str,  # noqa: A002
        client: ApifyClientAsync | MemoryStorageClient,
    ) -> RequestQueueClientAsync | RequestQueueClient:
        return client.request_queue(id)

    @classmethod
    def _get_storage_collection_client(
        cls: type[RequestQueue],
        client: ApifyClientAsync | MemoryStorageClient,
    ) -> RequestQueueCollectionClientAsync | RequestQueueCollectionClient:
        return client.request_queues()

    async def add_request(
        self: RequestQueue,
        request: dict,
        *,
        forefront: bool = False,
        keep_url_fragment: bool = False,
        use_extended_unique_key: bool = False,
    ) -> dict:
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
            - `requestId` (str): The ID of the request.
            - `uniqueKey` (str): The unique key associated with the request.
            - `wasAlreadyPresent` (bool): Indicates whether the request was already in the queue.
            - `wasAlreadyHandled` (bool): Indicates whether the request was already processed.
        """
        budget_ow(
            request,
            {
                'url': (str, True),
            },
        )
        self._last_activity = datetime.now(timezone.utc)

        if request.get('uniqueKey') is None:
            request['uniqueKey'] = compute_unique_key(
                url=request['url'],
                method=request.get('method', 'GET'),
                payload=request.get('payload'),
                keep_url_fragment=keep_url_fragment,
                use_extended_unique_key=use_extended_unique_key,
            )

        cache_key = unique_key_to_request_id(request['uniqueKey'])
        cached_info = self._requests_cache.get(cache_key)

        if cached_info:
            request['id'] = cached_info['id']
            return {
                'wasAlreadyPresent': True,
                # We may assume that if request is in local cache then also the information if the
                # request was already handled is there because just one client should be using one queue.
                'wasAlreadyHandled': cached_info['isHandled'],
                'requestId': cached_info['id'],
                'uniqueKey': cached_info['uniqueKey'],
            }

        queue_operation_info = await self._request_queue_client.add_request(request, forefront=forefront)
        queue_operation_info['uniqueKey'] = request['uniqueKey']

        self._cache_request(cache_key, queue_operation_info)

        request_id, was_already_present = queue_operation_info['requestId'], queue_operation_info['wasAlreadyPresent']
        is_handled = request.get('handledAt') is not None
        if not is_handled and not was_already_present and request_id not in self._in_progress and self._recently_handled.get(request_id) is None:
            self._assumed_total_count += 1

            self._maybe_add_request_to_queue_head(request_id, forefront)

        return queue_operation_info

    async def get_request(self: RequestQueue, request_id: str) -> dict | None:
        """Retrieve a request from the queue.

        Args:
            request_id (str): ID of the request to retrieve.

        Returns:
            dict, optional: The retrieved request, or `None`, if it does not exist.
        """
        budget_ow(request_id, (str, True), 'request_id')
        return await self._request_queue_client.get_request(request_id)

    async def fetch_next_request(self: RequestQueue) -> dict | None:
        """Return the next request in the queue to be processed.

        Once you successfully finish processing of the request, you need to call
        `RequestQueue.mark_request_as_handled` to mark the request as handled in the queue.
        If there was some error in processing the request, call `RequestQueue.reclaim_request` instead,
        so that the queue will give the request to some other consumer in another call to the `fetch_next_request` method.

        Note that the `None` return value does not mean the queue processing finished, it means there are currently no pending requests.
        To check whether all requests in queue were finished, use `RequestQueue.is_finished` instead.

        Returns:
            dict, optional: The request or `None` if there are no more pending requests.
        """
        await self._ensure_head_is_non_empty()

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

        # NOTE: It can happen that the queue head index is inconsistent with the main queue table. This can occur in two situations:

        """ 1) Queue head index is ahead of the main table and the request is not present in the main table yet (i.e. getRequest() returned null).
                In this case, keep the request marked as in progress for a short while,
                so that isFinished() doesn't return true and _ensureHeadIsNonEmpty() doesn't not load the request
                into the queueHeadDict straight again. After the interval expires, fetchNextRequest()
                will try to fetch this request again, until it eventually appears in the main table.
        """
        if request is None:
            logger.debug('Cannot find a request from the beginning of queue, will be retried later', extra={'nextRequestId': next_request_id})
            asyncio.get_running_loop().call_later(STORAGE_CONSISTENCY_DELAY_MILLIS // 1000, lambda: self._in_progress.remove(next_request_id))
            return None

        """ 2) Queue head index is behind the main table and the underlying request was already handled
               (by some other client, since we keep the track of handled requests in recentlyHandled dictionary).
               We just add the request to the recentlyHandled dictionary so that next call to _ensureHeadIsNonEmpty()
               will not put the request again to queueHeadDict.
        """
        if request.get('handledAt') is not None:
            logger.debug('Request fetched from the beginning of queue was already handled', extra={'nextRequestId': next_request_id})
            self._recently_handled[next_request_id] = True
            return None

        return request

    async def mark_request_as_handled(self: RequestQueue, request: dict) -> dict | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `RequestQueue.fetch_next_request` method.

        Args:
            request (dict): The request to mark as handled.

        Returns:
            dict, optional: Information about the queue operation with keys `requestId`, `uniqueKey`, `wasAlreadyPresent`, `wasAlreadyHandled`.
                `None` if the given request was not in progress.
        """
        budget_ow(
            request,
            {
                'id': (str, True),
                'uniqueKey': (str, True),
                'handledAt': (datetime, False),
            },
        )
        self._last_activity = datetime.now(timezone.utc)
        if request['id'] not in self._in_progress:
            logger.debug('Cannot mark request as handled, because it is not in progress!', extra={'requestId': request['id']})
            return None

        request['handledAt'] = request.get('handledAt', datetime.now(timezone.utc))
        queue_operation_info = await self._request_queue_client.update_request({**request})
        queue_operation_info['uniqueKey'] = request['uniqueKey']

        self._in_progress.remove(request['id'])
        self._recently_handled[request['id']] = True

        if not queue_operation_info['wasAlreadyHandled']:
            self._assumed_handled_count += 1

        self._cache_request(unique_key_to_request_id(request['uniqueKey']), queue_operation_info)

        return queue_operation_info

    async def reclaim_request(
        self: RequestQueue,
        request: dict,
        forefront: bool = False,  # noqa: FBT001, FBT002
    ) -> dict | None:
        """Reclaim a failed request back to the queue.

        The request will be returned for processing later again
        by another call to `RequestQueue.fetchNextRequest`.

        Args:
            request (dict): The request to return to the queue.
            forefront (bool, optional): Whether to add the request to the head or the end of the queue
        Returns:
            dict, optional: Information about the queue operation with keys `requestId`, `uniqueKey`, `wasAlreadyPresent`, `wasAlreadyHandled`.
                `None` if the given request was not in progress.
        """
        budget_ow(
            request,
            {
                'id': (str, True),
                'uniqueKey': (str, True),
            },
        )
        self._last_activity = datetime.now(timezone.utc)

        if request['id'] not in self._in_progress:
            logger.debug('Cannot reclaim request, because it is not in progress!', extra={'requestId': request['id']})
            return None

        # TODO: If request hasn't been changed since the last getRequest(), we don't need to call updateRequest()
        # and thus improve performance.
        # https://github.com/apify/apify-sdk-python/issues/143
        queue_operation_info = await self._request_queue_client.update_request(request, forefront=forefront)
        queue_operation_info['uniqueKey'] = request['uniqueKey']
        self._cache_request(unique_key_to_request_id(request['uniqueKey']), queue_operation_info)

        # Wait a little to increase a chance that the next call to fetchNextRequest() will return the request with updated data.
        # This is to compensate for the limitation of DynamoDB, where writes might not be immediately visible to subsequent reads.
        def callback() -> None:
            if request['id'] not in self._in_progress:
                logger.debug('The request is no longer marked as in progress in the queue?!', {'requestId': request['id']})
                return

            self._in_progress.remove(request['id'])

            # Performance optimization: add request straight to head if possible
            self._maybe_add_request_to_queue_head(request['id'], forefront)

        asyncio.get_running_loop().call_later(STORAGE_CONSISTENCY_DELAY_MILLIS // 1000, callback)

        return queue_operation_info

    def _in_progress_count(self: RequestQueue) -> int:
        return len(self._in_progress)

    async def is_empty(self: RequestQueue) -> bool:
        """Check whether the queue is empty.

        Returns:
            bool: `True` if the next call to `RequestQueue.fetchNextRequest` would return `None`, otherwise `False`.
        """
        await self._ensure_head_is_non_empty()
        return len(self._queue_head_dict) == 0

    async def is_finished(self: RequestQueue) -> bool:
        """Check whether the queue is finished.

        Due to the nature of distributed storage used by the queue,
        the function might occasionally return a false negative,
        but it will never return a false positive.

        Returns:
            bool: `True` if all requests were already handled and there are no more left. `False` otherwise.
        """
        seconds_since_last_activity = (datetime.now(timezone.utc) - self._last_activity).seconds
        if self._in_progress_count() > 0 and seconds_since_last_activity > self._internal_timeout_seconds:
            message = f'The request queue seems to be stuck for {self._internal_timeout_seconds}s, resetting internal state.'
            logger.warning(message)
            self._reset()

        if len(self._queue_head_dict) > 0 or self._in_progress_count() > 0:
            return False

        is_head_consistent = await self._ensure_head_is_non_empty(ensure_consistency=True)
        return is_head_consistent and len(self._queue_head_dict) == 0 and self._in_progress_count() == 0

    def _reset(self: RequestQueue) -> None:
        self._queue_head_dict.clear()
        self._query_queue_head_task = None
        self._in_progress.clear()
        self._recently_handled.clear()
        self._assumed_total_count = 0
        self._assumed_handled_count = 0
        self._requests_cache.clear()
        self._last_activity = datetime.now(timezone.utc)

    def _cache_request(self: RequestQueue, cache_key: str, queue_operation_info: dict) -> None:
        self._requests_cache[cache_key] = {
            'id': queue_operation_info['requestId'],
            'isHandled': queue_operation_info['wasAlreadyHandled'],
            'uniqueKey': queue_operation_info['uniqueKey'],
            'wasAlreadyHandled': queue_operation_info['wasAlreadyHandled'],
        }

    async def _queue_query_head(self: RequestQueue, limit: int) -> dict:
        query_started_at = datetime.now(timezone.utc)

        list_head = await self._request_queue_client.list_head(limit=limit)
        for request in list_head['items']:
            # Queue head index might be behind the main table, so ensure we don't recycle requests
            if not request['id'] or not request['uniqueKey'] or request['id'] in self._in_progress or self._recently_handled.get(request['id']):
                continue
            self._queue_head_dict[request['id']] = request['id']
            self._cache_request(
                unique_key_to_request_id(request['uniqueKey']),
                {
                    'requestId': request['id'],
                    'wasAlreadyHandled': False,
                    'wasAlreadyPresent': True,
                    'uniqueKey': request['uniqueKey'],
                },
            )

        # This is needed so that the next call to _ensureHeadIsNonEmpty() will fetch the queue head again.
        self._query_queue_head_task = None

        return {
            'wasLimitReached': len(list_head['items']) >= limit,
            'prevLimit': limit,
            'queueModifiedAt': list_head['queueModifiedAt'],
            'queryStartedAt': query_started_at,
            'hadMultipleClients': list_head['hadMultipleClients'],
        }

    async def _ensure_head_is_non_empty(
        self: RequestQueue,
        ensure_consistency: bool = False,  # noqa: FBT001, FBT002
        limit: int | None = None,
        iteration: int = 0,
    ) -> bool:
        # If is nonempty resolve immediately.
        if len(self._queue_head_dict) > 0:
            return True

        if limit is None:
            limit = max(self._in_progress_count() * QUERY_HEAD_BUFFER, QUERY_HEAD_MIN_LENGTH)

        if self._query_queue_head_task is None:
            self._query_queue_head_task = asyncio.Task(self._queue_query_head(limit))

        queue_head = await self._query_queue_head_task

        # TODO: I feel this code below can be greatly simplified... (comes from TS implementation *wink*)
        # https://github.com/apify/apify-sdk-python/issues/142

        # If queue is still empty then one of the following holds:
        # - the other calls waiting for this task already consumed all the returned requests
        # - the limit was too low and contained only requests in progress
        # - the writes from other clients were not propagated yet
        # - the whole queue was processed and we are done

        # If limit was not reached in the call then there are no more requests to be returned.
        if queue_head['prevLimit'] >= REQUEST_QUEUE_HEAD_MAX_LIMIT:
            logger.warning('Reached the maximum number of requests in progress', extra={'limit': REQUEST_QUEUE_HEAD_MAX_LIMIT})

        should_repeat_with_higher_limit = (
            len(self._queue_head_dict) == 0 and queue_head['wasLimitReached'] and queue_head['prevLimit'] < REQUEST_QUEUE_HEAD_MAX_LIMIT
        )

        # If ensureConsistency=true then we must ensure that either:
        # - queueModifiedAt is older than queryStartedAt by at least API_PROCESSED_REQUESTS_DELAY_MILLIS
        # - hadMultipleClients=false and this.assumedTotalCount<=this.assumedHandledCount
        is_database_consistent = (queue_head['queryStartedAt'] - queue_head['queueModifiedAt'].replace(tzinfo=timezone.utc)).seconds >= (
            API_PROCESSED_REQUESTS_DELAY_MILLIS // 1000
        )
        is_locally_consistent = not queue_head['hadMultipleClients'] and self._assumed_total_count <= self._assumed_handled_count
        # Consistent information from one source is enough to consider request queue finished.
        should_repeat_for_consistency = ensure_consistency and not is_database_consistent and not is_locally_consistent

        # If both are false then head is consistent and we may exit.
        if not should_repeat_with_higher_limit and not should_repeat_for_consistency:
            return True

        # If we are querying for consistency then we limit the number of queries to MAX_QUERIES_FOR_CONSISTENCY.
        # If this is reached then we return false so that empty() and finished() returns possibly false negative.
        if not should_repeat_with_higher_limit and iteration > MAX_QUERIES_FOR_CONSISTENCY:
            return False

        next_limit = round(queue_head['prevLimit'] * 1.5) if should_repeat_with_higher_limit else queue_head['prevLimit']

        # If we are repeating for consistency then wait required time.
        if should_repeat_for_consistency:
            delay_seconds = (API_PROCESSED_REQUESTS_DELAY_MILLIS // 1000) - (datetime.now(timezone.utc) - queue_head['queueModifiedAt']).seconds
            logger.info(f'Waiting for {delay_seconds}s before considering the queue as finished to ensure that the data is consistent.')
            await asyncio.sleep(delay_seconds)

        return await self._ensure_head_is_non_empty(ensure_consistency, next_limit, iteration + 1)

    def _maybe_add_request_to_queue_head(
        self: RequestQueue,
        request_id: str,
        forefront: bool,  # noqa: FBT001
    ) -> None:
        if forefront:
            self._queue_head_dict[request_id] = request_id
            # Move to start, i.e. forefront of the queue
            self._queue_head_dict.move_to_end(request_id, last=False)
        elif self._assumed_total_count < QUERY_HEAD_MIN_LENGTH:
            # OrderedDict puts the item to the end of the queue by default
            self._queue_head_dict[request_id] = request_id

    async def drop(self: RequestQueue) -> None:
        """Remove the request queue either from the Apify cloud storage or from the local directory."""
        await self._request_queue_client.delete()
        self._remove_from_cache()

    async def get_info(self: RequestQueue) -> dict | None:
        """Get an object containing general information about the request queue.

        Returns:
            dict: Object returned by calling the GET request queue API endpoint.
        """
        return await self._request_queue_client.get()

    @classmethod
    async def open(
        cls: type[RequestQueue],
        *,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
        force_cloud: bool = False,
        config: Configuration | None = None,
    ) -> RequestQueue:
        """Open a request queue.

        Request queue represents a queue of URLs to crawl, which is stored either on local filesystem or in the Apify cloud.
        The queue is used for deep crawling of websites, where you start with several URLs and then
        recursively follow links to other pages. The data structure supports both breadth-first
        and depth-first crawling orders.

        Args:
            id (str, optional): ID of the request queue to be opened.
                If neither `id` nor `name` are provided, the method returns the default request queue associated with the actor run.
                If the request queue with the given ID does not exist, it raises an error.
            name (str, optional): Name of the request queue to be opened.
                If neither `id` nor `name` are provided, the method returns the default request queue associated with the actor run.
                If the request queue with the given name does not exist, it is created.
            force_cloud (bool, optional): If set to True, it will open a request queue on the Apify Platform even when running the actor locally.
                Defaults to False.
            config (Configuration, optional): A `Configuration` instance, uses global configuration if omitted.

        Returns:
            RequestQueue: An instance of the `RequestQueue` class for the given ID or name.
        """
        queue = await super().open(id=id, name=name, force_cloud=force_cloud, config=config)
        await queue._ensure_head_is_non_empty()  # type: ignore
        return queue  # type: ignore
