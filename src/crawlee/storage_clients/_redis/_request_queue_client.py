from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import NotRequired, override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

from ._client_mixin import MetadataUpdateParams, RedisClientMixin
from ._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import Sequence

    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline
    from redis.commands.core import AsyncScript

logger = getLogger(__name__)


class _QueueMetadataUpdateParams(MetadataUpdateParams):
    """Parameters for updating queue metadata."""

    new_handled_request_count: NotRequired[int]
    new_pending_request_count: NotRequired[int]
    new_total_request_count: NotRequired[int]
    delta_handled_request_count: NotRequired[int]
    delta_pending_request_count: NotRequired[int]
    delta_total_request_count: NotRequired[int]
    recalculate: NotRequired[bool]
    update_had_multiple_clients: NotRequired[bool]


class RedisRequestQueueClient(RequestQueueClient, RedisClientMixin):
    """Redis implementation of the request queue client.

    This client persists requests to Redis using multiple data structures for efficient queue operations,
    deduplication, and concurrent access safety. Requests are stored with FIFO ordering and support
    both regular and forefront (high-priority) insertion modes.

    The implementation uses Bloom filters for efficient request deduplication and Redis lists for
    queue operations. Request blocking and client coordination is handled through Redis hashes
    with timestamp-based expiration for stale request recovery.

    The request queue data is stored in Redis using the following key patterns:
    - `request_queues:{name}:queue` - Redis list for FIFO request ordering
    - `request_queues:{name}:data` - Redis hash storing serialized Request objects by unique_key
    - `request_queues:{name}:in_progress` - Redis hash tracking requests currently being processed
    - `request_queues:{name}:added_bloom_filter` - Bloom filter for added request deduplication (`bloom` dedup_strategy)
    - `request_queues:{name}:handled_bloom_filter` - Bloom filter for completed request tracking (`bloom`
        dedup_strategy)
    - `request_queues:{name}:pending_set` - Redis set for added request deduplication (`default` dedup_strategy)
    - `request_queues:{name}:handled_set` - Redis set for completed request tracking (`default` dedup_strategy)
    - `request_queues:{name}:metadata` - Redis JSON object containing queue metadata

    Requests are serialized to JSON for storage and maintain proper FIFO ordering through Redis list
    operations. The implementation provides concurrent access safety through atomic Lua scripts,
    Bloom filter operations, and Redis's built-in atomicity guarantees for individual operations.
    """

    _DEFAULT_NAME = 'default'
    """Default Request Queue name key prefix when none provided."""

    _MAIN_KEY = 'request_queues'
    """Main Redis key prefix for Request Queue."""

    _CLIENT_TYPE = 'Request queue'
    """Human-readable client type for error messages."""

    _MAX_BATCH_FETCH_SIZE = 10
    """Maximum number of requests to fetch in a single batch operation."""

    _BLOCK_REQUEST_TIME = 300_000  # milliseconds
    """Time in milliseconds to block a fetched request for other clients before it can be autoreclaimed."""

    _RECLAIM_INTERVAL = timedelta(seconds=30)
    """Interval to check for stale requests to reclaim."""

    def __init__(
        self,
        storage_name: str,
        storage_id: str,
        redis: Redis,
        dedup_strategy: Literal['default', 'bloom'] = 'default',
        bloom_error_rate: float = 1e-7,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `RedisRequestQueueClient.open` class method to create a new instance.
        """
        super().__init__(storage_name=storage_name, storage_id=storage_id, redis=redis)

        self._dedup_strategy = dedup_strategy
        """Deduplication strategy for the queue."""

        self._bloom_error_rate = bloom_error_rate
        """Desired false positive rate for Bloom filters."""

        self._pending_fetch_cache: deque[Request] = deque()
        """Cache for requests: ordered by sequence number."""

        self.client_key = crypto_random_object_id(length=32)[:32]
        """Unique identifier for this client instance."""

        # Lua scripts for atomic operations
        self._fetch_script: AsyncScript | None = None
        self._reclaim_stale_script: AsyncScript | None = None
        self._add_requests_script: AsyncScript | None = None

        self._next_reclaim_stale: None | datetime = None

    @property
    def _added_filter_key(self) -> str:
        """Return the Redis key for the added requests Bloom filter."""
        if self._dedup_strategy != 'bloom':
            raise RuntimeError('The added requests filter is only available with the bloom deduplication strategy.')
        return f'{self._MAIN_KEY}:{self._storage_name}:added_bloom_filter'

    @property
    def _handled_filter_key(self) -> str:
        """Return the Redis key for the handled requests Bloom filter."""
        if self._dedup_strategy != 'bloom':
            raise RuntimeError('The handled requests filter is only available with the bloom deduplication strategy.')
        return f'{self._MAIN_KEY}:{self._storage_name}:handled_bloom_filter'

    @property
    def _pending_set_key(self) -> str:
        """Return the Redis key for the pending requests set."""
        if self._dedup_strategy != 'default':
            raise RuntimeError('The pending requests set is only available with the default deduplication strategy.')
        return f'{self._MAIN_KEY}:{self._storage_name}:pending_set'

    @property
    def _handled_set_key(self) -> str:
        """Return the Redis key for the handled requests set."""
        if self._dedup_strategy != 'default':
            raise RuntimeError('The handled requests set is only available with the default deduplication strategy.')
        return f'{self._MAIN_KEY}:{self._storage_name}:handled_set'

    @property
    def _queue_key(self) -> str:
        """Return the Redis key for the request queue."""
        return f'{self._MAIN_KEY}:{self._storage_name}:queue'

    @property
    def _data_key(self) -> str:
        """Return the Redis key for the request data hash."""
        return f'{self._MAIN_KEY}:{self._storage_name}:data'

    @property
    def _in_progress_key(self) -> str:
        """Return the Redis key for the in-progress requests hash."""
        return f'{self._MAIN_KEY}:{self._storage_name}:in_progress'

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        redis: Redis,
        dedup_strategy: Literal['default', 'bloom'] = 'default',
        bloom_error_rate: float = 1e-7,
    ) -> RedisRequestQueueClient:
        """Open or create a new Redis request queue client.

        This method attempts to open an existing request queue from the Redis database. If a queue with the specified
        ID or name exists, it loads the metadata from the database. If no existing queue is found, a new one
        is created.

        Args:
            id: The ID of the request queue. If not provided, a random ID will be generated.
            name: The name of the dataset for named (global scope) storages.
            alias: The alias of the dataset for unnamed (run scope) storages.
            redis: Redis client instance.
            dedup_strategy: Strategy for request queue deduplication. Options are:
                - 'default': Uses Redis sets for exact deduplication.
                - 'bloom': Uses Redis Bloom filters for probabilistic deduplication with lower memory usage. When using
                    this approach, there is a possibility 1e-7 that requests will be skipped in the queue.
            bloom_error_rate: Desired false positive rate for Bloom filter deduplication. Only relevant if
                `dedup_strategy` is set to 'bloom'.

        Returns:
            An instance for the opened or created storage client.
        """
        return await cls._open(
            id=id,
            name=name,
            alias=alias,
            redis=redis,
            metadata_model=RequestQueueMetadata,
            extra_metadata_fields={
                'had_multiple_clients': False,
                'handled_request_count': 0,
                'pending_request_count': 0,
                'total_request_count': 0,
            },
            instance_kwargs={'dedup_strategy': dedup_strategy, 'bloom_error_rate': bloom_error_rate},
        )

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        return await self._get_metadata(RequestQueueMetadata)

    @override
    async def drop(self) -> None:
        if self._dedup_strategy == 'bloom':
            extra_keys = [self._added_filter_key, self._handled_filter_key]
        elif self._dedup_strategy == 'default':
            extra_keys = [self._pending_set_key, self._handled_set_key]
        else:
            raise RuntimeError(f'Unknown deduplication strategy: {self._dedup_strategy}')
        extra_keys.extend([self._queue_key, self._data_key, self._in_progress_key])
        await self._drop(extra_keys=extra_keys)

    @override
    async def purge(self) -> None:
        if self._dedup_strategy == 'bloom':
            extra_keys = [self._added_filter_key, self._handled_filter_key]
        elif self._dedup_strategy == 'default':
            extra_keys = [self._pending_set_key, self._handled_set_key]
        else:
            raise RuntimeError(f'Unknown deduplication strategy: {self._dedup_strategy}')
        extra_keys.extend([self._queue_key, self._data_key, self._in_progress_key])
        await self._purge(
            extra_keys=extra_keys,
            metadata_kwargs=_QueueMetadataUpdateParams(
                update_accessed_at=True,
                update_modified_at=True,
                new_pending_request_count=0,
            ),
        )

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        # Mypy workaround
        if self._add_requests_script is None:
            raise RuntimeError('Scripts not loaded. Call _ensure_scripts_loaded() before using the client.')

        processed_requests = []

        delta_pending = 0
        delta_total = 0

        requests_by_unique_key = {req.unique_key: req for req in requests}
        unique_keys = list(requests_by_unique_key.keys())
        # Check which requests are already added or handled
        async with self._get_pipeline(with_execute=False) as pipe:
            if self._dedup_strategy == 'default':
                await await_redis_response(pipe.smismember(self._pending_set_key, unique_keys))
                await await_redis_response(pipe.smismember(self._handled_set_key, unique_keys))
            elif self._dedup_strategy == 'bloom':
                await await_redis_response(pipe.bf().mexists(self._added_filter_key, *unique_keys))  # type: ignore[no-untyped-call]
                await await_redis_response(pipe.bf().mexists(self._handled_filter_key, *unique_keys))  # type: ignore[no-untyped-call]

            pipe_results = await pipe.execute()

        added_pending_flags = pipe_results[0]
        handled_flags = pipe_results[1]

        new_unique_keys = []
        new_request_data = {}
        delta_pending = 0
        delta_total = 0

        for i, unique_key in enumerate(unique_keys):
            # Already handled - skip
            if handled_flags[i]:
                processed_requests.append(
                    ProcessedRequest(
                        unique_key=unique_key,
                        was_already_present=True,
                        was_already_handled=True,
                    )
                )
                continue

            # Already in queue - skip
            if added_pending_flags[i]:
                processed_requests.append(
                    ProcessedRequest(
                        unique_key=unique_key,
                        was_already_present=True,
                        was_already_handled=False,
                    )
                )
                continue

            # New request - will add to queue
            request = requests_by_unique_key[unique_key]

            new_unique_keys.append(unique_key)
            new_request_data[unique_key] = request.model_dump_json()

        if new_unique_keys:
            # Add new requests to the queue atomically, get back which were actually added
            script_results = await self._add_requests_script(
                keys=[
                    self._added_filter_key if self._dedup_strategy == 'bloom' else self._pending_set_key,
                    self._queue_key,
                    self._data_key,
                ],
                args=[int(forefront), json.dumps(new_unique_keys), json.dumps(new_request_data)],
            )
            actually_added = set(json.loads(script_results))

            delta_pending = len(actually_added)
            delta_total = len(actually_added)

            processed_requests.extend(
                [
                    ProcessedRequest(
                        unique_key=unique_key,
                        was_already_present=unique_key not in actually_added,
                        was_already_handled=False,
                    )
                    for unique_key in new_unique_keys
                ]
            )

        async with self._get_pipeline() as pipe:
            await self._update_metadata(
                pipe,
                **_QueueMetadataUpdateParams(
                    update_accessed_at=True,
                    update_modified_at=True,
                    delta_pending_request_count=delta_pending,
                    delta_total_request_count=delta_total,
                ),
            )

        return AddRequestsResponse(
            processed_requests=processed_requests,
            unprocessed_requests=[],
        )

    @override
    async def fetch_next_request(self) -> Request | None:
        if self._pending_fetch_cache:
            return self._pending_fetch_cache.popleft()

        # Mypy workaround
        if self._fetch_script is None:
            raise RuntimeError('Scripts not loaded. Call _ensure_scripts_loaded() before using the client.')

        blocked_until_timestamp = int(datetime.now(tz=timezone.utc).timestamp() * 1000) + self._BLOCK_REQUEST_TIME

        # The script retrieves requests from the queue and places them in the in_progress hash.
        requests_json = await self._fetch_script(
            keys=[self._queue_key, self._in_progress_key, self._data_key],
            args=[self.client_key, blocked_until_timestamp, self._MAX_BATCH_FETCH_SIZE],
        )

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, **_QueueMetadataUpdateParams(update_accessed_at=True))

        if not requests_json:
            return None

        requests = [Request.model_validate_json(req_json) for req_json in requests_json]

        self._pending_fetch_cache.extend(requests[1:])

        return requests[0]

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        request_data = await await_redis_response(self._redis.hget(self._data_key, unique_key))

        if isinstance(request_data, (str, bytes, bytearray)):
            return Request.model_validate_json(request_data)

        return None

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        # Check if the request is in progress.
        check_in_progress = await await_redis_response(self._redis.hexists(self._in_progress_key, request.unique_key))
        if not check_in_progress:
            logger.warning(f'Marking request {request.unique_key} as handled that is not in progress.')
            return None

        async with self._get_pipeline() as pipe:
            if self._dedup_strategy == 'default':
                await await_redis_response(pipe.sadd(self._handled_set_key, request.unique_key))
                await await_redis_response(pipe.srem(self._pending_set_key, request.unique_key))
            elif self._dedup_strategy == 'bloom':
                await await_redis_response(pipe.bf().add(self._handled_filter_key, request.unique_key))  # type: ignore[no-untyped-call]

            await await_redis_response(pipe.hdel(self._in_progress_key, request.unique_key))
            await await_redis_response(pipe.hdel(self._data_key, request.unique_key))

            await self._update_metadata(
                pipe,
                **_QueueMetadataUpdateParams(
                    update_accessed_at=True,
                    update_modified_at=True,
                    delta_handled_request_count=1,
                    delta_pending_request_count=-1,
                ),
            )

        return ProcessedRequest(
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
        check_in_progress = await await_redis_response(self._redis.hexists(self._in_progress_key, request.unique_key))
        if not check_in_progress:
            logger.info(f'Reclaiming request {request.unique_key} that is not in progress.')
            return None

        async with self._get_pipeline() as pipe:
            if forefront:
                blocked_until_timestamp = (
                    int(datetime.now(tz=timezone.utc).timestamp() * 1000) + self._BLOCK_REQUEST_TIME
                )

                await await_redis_response(
                    pipe.hset(
                        self._in_progress_key,
                        request.unique_key,
                        f'{{"client_id":"{self.client_key}","blocked_until_timestamp":{blocked_until_timestamp}}}',
                    )
                )
                self._pending_fetch_cache.appendleft(request)
            else:
                await await_redis_response(pipe.rpush(self._queue_key, request.unique_key))
                await await_redis_response(pipe.hset(self._data_key, request.unique_key, request.model_dump_json()))
                await await_redis_response(pipe.hdel(self._in_progress_key, request.unique_key))
            await self._update_metadata(
                pipe,
                **_QueueMetadataUpdateParams(
                    update_modified_at=True,
                    update_accessed_at=True,
                ),
            )

        return ProcessedRequest(
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
        if self._pending_fetch_cache:
            return False

        # Reclaim stale requests if needed
        if self._next_reclaim_stale is None or datetime.now(tz=timezone.utc) >= self._next_reclaim_stale:
            await self._reclaim_stale_requests()
            self._next_reclaim_stale = datetime.now(tz=timezone.utc) + self._RECLAIM_INTERVAL

        metadata = await self.get_metadata()

        return metadata.pending_request_count == 0

    async def _load_scripts(self) -> None:
        """Ensure Lua scripts are loaded in Redis."""
        self._fetch_script = await self._create_script('atomic_fetch_request.lua')
        self._reclaim_stale_script = await self._create_script('reclaim_stale_requests.lua')
        if self._dedup_strategy == 'bloom':
            self._add_requests_script = await self._create_script('atomic_bloom_add_requests.lua')
        elif self._dedup_strategy == 'default':
            self._add_requests_script = await self._create_script('atomic_set_add_requests.lua')

    @override
    async def _create_storage(self, pipeline: Pipeline) -> None:
        # Create Bloom filters for added and handled requests
        if self._dedup_strategy == 'bloom':
            await await_redis_response(
                pipeline.bf().create(
                    self._added_filter_key, errorRate=self._bloom_error_rate, capacity=100000, expansion=10
                )  # type: ignore[no-untyped-call]
            )
            await await_redis_response(
                pipeline.bf().create(
                    self._handled_filter_key, errorRate=self._bloom_error_rate, capacity=100000, expansion=10
                )  # type: ignore[no-untyped-call]
            )

    async def _reclaim_stale_requests(self) -> None:
        """Reclaim requests that have been in progress for too long."""
        # Mypy workaround
        if self._reclaim_stale_script is None:
            raise RuntimeError('Scripts not loaded. Call _ensure_scripts_loaded() before using the client.')

        current_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        await self._reclaim_stale_script(
            keys=[self._in_progress_key, self._queue_key, self._data_key], args=[current_time]
        )

    @override
    async def _specific_update_metadata(
        self,
        pipeline: Pipeline,
        *,
        delta_handled_request_count: int | None = None,
        new_handled_request_count: int | None = None,
        delta_pending_request_count: int | None = None,
        new_pending_request_count: int | None = None,
        delta_total_request_count: int | None = None,
        new_total_request_count: int | None = None,
        update_had_multiple_clients: bool = False,
        **_kwargs: Any,
    ) -> None:
        """Update the dataset metadata with current information.

        Args:
            pipeline: The Redis pipeline to use for the update.
            new_handled_request_count: If provided, update the handled_request_count to this value.
            new_pending_request_count: If provided, update the pending_request_count to this value.
            new_total_request_count: If provided, update the total_request_count to this value.
            delta_handled_request_count: If provided, add this value to the handled_request_count.
            delta_pending_request_count: If provided, add this value to the pending_request_count.
            delta_total_request_count: If provided, add this value to the total_request_count.
            update_had_multiple_clients: If True, set had_multiple_clients to True.
        """
        if new_pending_request_count is not None:
            await await_redis_response(
                pipeline.json().set(
                    self.metadata_key, '$.pending_request_count', new_pending_request_count, nx=False, xx=True
                )
            )
        elif delta_pending_request_count is not None:
            await await_redis_response(
                pipeline.json().numincrby(self.metadata_key, '$.pending_request_count', delta_pending_request_count)
            )

        if new_handled_request_count is not None:
            await await_redis_response(
                pipeline.json().set(
                    self.metadata_key, '$.handled_request_count', new_handled_request_count, nx=False, xx=True
                )
            )
        elif delta_handled_request_count is not None:
            await await_redis_response(
                pipeline.json().numincrby(self.metadata_key, '$.handled_request_count', delta_handled_request_count)
            )

        if new_total_request_count is not None:
            await await_redis_response(
                pipeline.json().set(
                    self.metadata_key, '$.total_request_count', new_total_request_count, nx=False, xx=True
                )
            )
        elif delta_total_request_count is not None:
            await await_redis_response(
                pipeline.json().numincrby(self.metadata_key, '$.total_request_count', delta_total_request_count)
            )

        if update_had_multiple_clients:
            await await_redis_response(
                pipeline.json().set(
                    self.metadata_key, '$.had_multiple_clients', update_had_multiple_clients, nx=False, xx=True
                )
            )
