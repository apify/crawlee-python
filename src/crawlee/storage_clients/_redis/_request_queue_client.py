from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

from ._client_mixin import RedisClientMixin
from ._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import Sequence

    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline
    from redis.commands.core import AsyncScript

logger = getLogger(__name__)


class RedisRequestQueueClient(RequestQueueClient, RedisClientMixin):
    """Memory implementation of the request queue client.

    No data is persisted between process runs, which means all requests are lost when the program terminates.
    This implementation is primarily useful for testing, development, and short-lived crawler runs where
    persistence is not required.

    This client provides fast access to request data but is limited by available memory and does not support
    data sharing across different processes.
    """

    _DEFAULT_NAME = 'default'

    _MAIN_KEY = 'request_queue'

    _CLIENT_TYPE = 'Request queue'
    """Human-readable client type for error messages."""

    _MAX_BATCH_FETCH_SIZE = 10

    _BLOCK_REQUEST_TIME = 300_000  # milliseconds

    def __init__(self, storage_name: str, storage_id: str, redis: Redis) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryDatasetClient.open` class method to create a new instance.
        """
        super().__init__(storage_name=storage_name, storage_id=storage_id, redis=redis)

        self._pending_fetch_cache: deque[Request] = deque()
        """Cache for requests: ordered by sequence number."""

        self.client_key = crypto_random_object_id(length=32)[:32]
        """Unique identifier for this client instance."""

        self._fetch_script: AsyncScript | None = None

        self._reclaim_stale_script: AsyncScript | None = None

        self._add_requests_script: AsyncScript | None = None

    @property
    def added_filter_key(self) -> str:
        """Return the Redis key for the added requests Bloom filter."""
        return f'{self._MAIN_KEY}:{self._storage_name}:added_bloom_filter'

    @property
    def handled_filter_key(self) -> str:
        """Return the Redis key for the handled requests Bloom filter."""
        return f'{self._MAIN_KEY}:{self._storage_name}:handled_bloom_filter'

    @property
    def queue_key(self) -> str:
        """Return the Redis key for the request queue."""
        return f'{self._MAIN_KEY}:{self._storage_name}:queue'

    @property
    def data_key(self) -> str:
        """Return the Redis key for the request data hash."""
        return f'{self._MAIN_KEY}:{self._storage_name}:data'

    @property
    def in_progress_key(self) -> str:
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
    ) -> RedisRequestQueueClient:
        """Open or create a new memory request queue client.

        This method creates a new in-memory request queue instance. Unlike persistent storage implementations,
        memory queues don't check for existing queues with the same name or ID since all data exists only
        in memory and is lost when the process terminates.

        Args:
            id: The ID of the request queue. If not provided, a random ID will be generated.
            name: The name of the dataset for named (global scope) storages.
            alias: The alias of the dataset for unnamed (run scope) storages.
            redis: Redis client instance.

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
        )

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        return await self._get_metadata(RequestQueueMetadata)

    @override
    async def drop(self) -> None:
        await self._drop(
            extra_keys=[
                self.added_filter_key,
                self.handled_filter_key,
                self.queue_key,
                self.data_key,
                self.in_progress_key,
            ]
        )

    @override
    async def purge(self) -> None:
        await self._purge(
            extra_keys=[
                self.added_filter_key,
                self.handled_filter_key,
                self.queue_key,
                self.data_key,
                self.in_progress_key,
            ],
            metadata_kwargs={
                'update_accessed_at': True,
                'update_modified_at': True,
                'new_pending_request_count': 0,
            },
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
        async with self._get_pipeline(with_execute=False) as pipe:
            await await_redis_response(pipe.bf().mexists(self.added_filter_key, *unique_keys))  # type: ignore[no-untyped-call]
            await await_redis_response(pipe.bf().mexists(self.handled_filter_key, *unique_keys))  # type: ignore[no-untyped-call]

            results = await pipe.execute()

        added_flags = results[0]
        handled_flags = results[1]

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
            if added_flags[i]:
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
            script_results = await self._add_requests_script(
                keys=[self.added_filter_key, self.queue_key, self.data_key],
                args=[int(forefront), json.dumps(new_unique_keys), json.dumps(new_request_data)],
            )
            actually_added = set(json.loads(script_results))

            delta_pending = len(actually_added)
            delta_total = len(actually_added)

            for unique_key in new_unique_keys:
                if unique_key in actually_added:
                    processed_requests.append(
                        ProcessedRequest(
                            unique_key=unique_key,
                            was_already_present=False,
                            was_already_handled=False,
                        )
                    )
                else:
                    processed_requests.append(
                        ProcessedRequest(
                            unique_key=unique_key,
                            was_already_present=True,
                            was_already_handled=False,
                        )
                    )

        async with self._get_pipeline() as pipe:
            await self._update_metadata(
                pipe,
                update_accessed_at=True,
                update_modified_at=True,
                delta_pending_request_count=delta_pending,
                delta_total_request_count=delta_total,
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

        requests_json = await self._fetch_script(
            keys=[self.queue_key, self.in_progress_key, self.data_key],
            args=[self.client_key, blocked_until_timestamp, self._MAX_BATCH_FETCH_SIZE],
        )

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, update_accessed_at=True)

        if not requests_json:
            return None

        requests = [Request.model_validate_json(req_json) for req_json in requests_json]

        self._pending_fetch_cache.extend(requests[1:])

        return requests[0]

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        request_data = await await_redis_response(self._redis.hget(self.data_key, unique_key))

        if isinstance(request_data, (str, bytes, bytearray)):
            return Request.model_validate_json(request_data)

        return None

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        # Check if the request is in progress.

        check_in_progress = await await_redis_response(self._redis.hexists(self.in_progress_key, request.unique_key))
        if not check_in_progress:
            logger.warning(f'Marking request {request.unique_key} as handled that is not in progress.')
            return None

        async with self._get_pipeline() as pipe:
            await await_redis_response(pipe.bf().add(self.handled_filter_key, request.unique_key))  # type: ignore[no-untyped-call]

            await await_redis_response(pipe.hdel(self.in_progress_key, request.unique_key))
            await await_redis_response(pipe.hdel(self.data_key, request.unique_key))

            await self._update_metadata(
                pipe,
                update_accessed_at=True,
                update_modified_at=True,
                delta_handled_request_count=1,
                delta_pending_request_count=-1,
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
        check_in_progress = await await_redis_response(self._redis.hexists(self.in_progress_key, request.unique_key))
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
                        self.in_progress_key,
                        request.unique_key,
                        f'{{"client_id":"{self.client_key}","blocked_until_timestamp":{blocked_until_timestamp}}}',
                    )
                )
                self._pending_fetch_cache.appendleft(request)
            else:
                await await_redis_response(pipe.rpush(self.queue_key, request.unique_key))
                await await_redis_response(pipe.hdel(self.in_progress_key, request.unique_key))
            await self._update_metadata(
                pipe,
                update_modified_at=True,
                update_accessed_at=True,
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

        metadata = await self.get_metadata()

        return metadata.pending_request_count == 0

    async def _load_scripts(self) -> None:
        """Ensure Lua scripts are loaded in Redis."""
        self._fetch_script = await self._create_script('atomic_fetch_request.lua')
        self._reclaim_stale_script = await self._create_script('reclaim_stale_requests.lua')
        self._add_requests_script = await self._create_script('atomic_add_requests.lua')

    @override
    async def _create_storage(self, pipeline: Pipeline) -> None:
        await await_redis_response(pipeline.bf().create(self.added_filter_key, 0.1e-7, 100000, expansion=10))  # type: ignore[no-untyped-call]
        await await_redis_response(pipeline.bf().create(self.handled_filter_key, 0.1e-7, 100000, expansion=10))  # type: ignore[no-untyped-call]

    async def _reclaim_stale_requests(self) -> None:
        # Mypy workaround
        if self._reclaim_stale_script is None:
            raise RuntimeError('Scripts not loaded. Call _ensure_scripts_loaded() before using the client.')

        current_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        await self._reclaim_stale_script(
            keys=[self.in_progress_key, self.queue_key, self.data_key], args=[current_time]
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
