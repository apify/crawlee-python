from __future__ import annotations

import json
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING

from redis.exceptions import ResponseError
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

    _MAX_BATCH_FETCH_SIZE = 10

    _BLOCK_REQUEST_TIME = 300_000  # milliseconds

    _DEFAULT_NAME = 'default'

    _MAIN_KEY = 'request_queue'

    def __init__(
        self,
        dataset_name: str,
        redis: Redis,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryDatasetClient.open` class method to create a new instance.
        """
        super().__init__(storage_name=dataset_name, redis=redis)

        self._pending_fetch_cache: deque[Request] = deque()
        """Cache for requests: ordered by sequence number."""

        self.client_key = crypto_random_object_id(length=32)[:32]
        """Unique identifier for this client instance."""

        self._fetch_script: AsyncScript | None = None

        self._reclaim_stale_script: AsyncScript | None = None

        self._add_requests_script: AsyncScript | None = None

        self._scripts_loaded = False

    async def _ensure_scripts_loaded(self) -> None:
        """Ensure Lua scripts are loaded in Redis."""
        if not self._scripts_loaded:
            self._fetch_script = await self._create_script('atomic_fetch_request.lua')
            self._reclaim_stale_script = await self._create_script('reclaim_stale_requests.lua')
            self._add_requests_script = await self._create_script('atomic_add_requests.lua')

            self._scripts_loaded = True

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        metadata_dict = await self._get_metadata_by_name(name=self._storage_name, redis=self._redis)
        if metadata_dict is None:
            raise ValueError(f'Dataset with name "{self._storage_name}" does not exist.')
        return RequestQueueMetadata.model_validate(metadata_dict)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        redis: Redis,
    ) -> RedisRequestQueueClient:
        """Open or create a new memory request queue client.

        This method creates a new in-memory request queue instance. Unlike persistent storage implementations,
        memory queues don't check for existing queues with the same name or ID since all data exists only
        in memory and is lost when the process terminates.

        Args:
            id: The ID of the request queue. If not provided, a random ID will be generated.
            name: The name of the request queue. If not provided, the queue will be unnamed.
            redis: Redis client instance.

        Returns:
            An instance for the opened or created storage client.
        """
        # Otherwise create a new queue
        if id:
            dataset_name = await cls._get_metadata_name_by_id(id=id, redis=redis)
            if dataset_name is None:
                raise ValueError(f'Dataset with ID "{id}" does not exist.')
        else:
            search_name = name or cls._DEFAULT_NAME
            metadata_data = await cls._get_metadata_by_name(name=search_name, redis=redis)
            dataset_name = search_name if metadata_data is not None else None
        if dataset_name:
            client = cls(dataset_name=dataset_name, redis=redis)
            async with client._get_pipeline() as pipe:
                await client._update_metadata(pipe, update_accessed_at=True)
        else:
            now = datetime.now(timezone.utc)
            metadata = RequestQueueMetadata(
                id=crypto_random_object_id(),
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
                had_multiple_clients=False,
                handled_request_count=0,
                pending_request_count=0,
                total_request_count=0,
            )
            dataset_name = name or cls._DEFAULT_NAME
            client = cls(dataset_name=dataset_name, redis=redis)
            with suppress(ResponseError):
                await client._create_metadata_and_storage(metadata.model_dump())

        await client._ensure_scripts_loaded()
        return client

    @override
    async def _create_storage(self, pipeline: Pipeline) -> None:
        added_bloom_filter_key = f'{self._MAIN_KEY}:{self._storage_name}:added_bloom_filter'
        handled_bloom_filter_key = f'{self._MAIN_KEY}:{self._storage_name}:handled_bloom_filter'
        await await_redis_response(pipeline.bf().create(added_bloom_filter_key, 0.1e-7, 100000, expansion=10))  # type: ignore[no-untyped-call]
        await await_redis_response(pipeline.bf().create(handled_bloom_filter_key, 0.1e-7, 100000, expansion=10))  # type: ignore[no-untyped-call]

    @override
    async def drop(self) -> None:
        storage_id = (await self.get_metadata()).id
        async with self._get_pipeline() as pipe:
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:metadata')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:added_bloom_filter')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:handled_bloom_filter')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:queue')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:data')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:in_progress')
            await pipe.delete(f'{self._MAIN_KEY}:id_to_name:{storage_id}')

    @override
    async def purge(self) -> None:
        async with self._get_pipeline() as pipe:
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:added_bloom_filter')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:handled_bloom_filter')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:queue')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:data')
            await pipe.delete(f'{self._MAIN_KEY}:{self._storage_name}:in_progress')

            await self._create_storage(pipe)

            await self._update_metadata(
                pipe,
                update_accessed_at=True,
                update_modified_at=True,
                new_pending_request_count=0,
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

        added_bloom_filter_key = f'{self._MAIN_KEY}:{self._storage_name}:added_bloom_filter'
        handled_bloom_filter_key = f'{self._MAIN_KEY}:{self._storage_name}:handled_bloom_filter'
        queue_key = f'{self._MAIN_KEY}:{self._storage_name}:queue'
        data_key = f'{self._MAIN_KEY}:{self._storage_name}:data'

        requests_by_unique_key = {req.unique_key: req for req in requests}
        unique_keys = list(requests_by_unique_key.keys())
        async with self._get_pipeline(with_execute=False) as pipe:
            await await_redis_response(pipe.bf().mexists(added_bloom_filter_key, *unique_keys))  # type: ignore[no-untyped-call]
            await await_redis_response(pipe.bf().mexists(handled_bloom_filter_key, *unique_keys))  # type: ignore[no-untyped-call]

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
                keys=[added_bloom_filter_key, queue_key, data_key],
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

        queue_key = f'{self._MAIN_KEY}:{self._storage_name}:queue'
        in_progress_key = f'{self._MAIN_KEY}:{self._storage_name}:in_progress'
        data_key = f'{self._MAIN_KEY}:{self._storage_name}:data'

        blocked_until_timestamp = int(datetime.now(tz=timezone.utc).timestamp() * 1000) + self._BLOCK_REQUEST_TIME

        requests_json = await self._fetch_script(
            keys=[queue_key, in_progress_key, data_key],
            args=[self.client_key, blocked_until_timestamp, self._MAX_BATCH_FETCH_SIZE],
        )

        async with self._get_pipeline() as pipe:
            await self._update_metadata(pipe, update_accessed_at=True)

        if not requests_json:
            return None

        requests = [Request.model_validate_json(req_json) for req_json in requests_json]

        self._pending_fetch_cache.extend(requests[1:])

        return requests[0]

    async def _reclaim_stale_requests(self) -> None:
        # Mypy workaround
        if self._reclaim_stale_script is None:
            raise RuntimeError('Scripts not loaded. Call _ensure_scripts_loaded() before using the client.')

        in_progress_key = f'{self._MAIN_KEY}:{self._storage_name}:in_progress'
        queue_key = f'{self._MAIN_KEY}:{self._storage_name}:queue'
        data_key = f'{self._MAIN_KEY}:{self._storage_name}:data'

        current_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        await self._reclaim_stale_script(keys=[in_progress_key, queue_key, data_key], args=[current_time])

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        data_key = f'{self._MAIN_KEY}:{self._storage_name}:data'

        request_data = await await_redis_response(self._redis.hget(data_key, unique_key))

        if isinstance(request_data, (str, bytes, bytearray)):
            return Request.model_validate_json(request_data)

        return None

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        # Check if the request is in progress.
        in_progress_key = f'{self._MAIN_KEY}:{self._storage_name}:in_progress'
        handled_bloom_filter_key = f'{self._MAIN_KEY}:{self._storage_name}:handled_bloom_filter'
        data_key = f'{self._MAIN_KEY}:{self._storage_name}:data'

        check_in_progress = await await_redis_response(self._redis.hexists(in_progress_key, request.unique_key))
        if not check_in_progress:
            logger.warning(f'Marking request {request.unique_key} as handled that is not in progress.')
            return None

        async with self._get_pipeline() as pipe:
            await await_redis_response(pipe.bf().add(handled_bloom_filter_key, request.unique_key))  # type: ignore[no-untyped-call]

            await await_redis_response(pipe.hdel(in_progress_key, request.unique_key))
            await await_redis_response(pipe.hdel(data_key, request.unique_key))

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
        in_progress_key = f'{self._MAIN_KEY}:{self._storage_name}:in_progress'
        queue_key = f'{self._MAIN_KEY}:{self._storage_name}:queue'

        check_in_progress = await await_redis_response(self._redis.hexists(in_progress_key, request.unique_key))
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
                        in_progress_key,
                        request.unique_key,
                        f'{{"client_id":"{self.client_key}","blocked_until_timestamp":{blocked_until_timestamp}}}',
                    )
                )
                self._pending_fetch_cache.appendleft(request)
            else:
                await await_redis_response(pipe.rpush(queue_key, request.unique_key))
                await await_redis_response(pipe.hdel(in_progress_key, request.unique_key))
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

    async def _update_metadata(
        self,
        pipeline: Pipeline,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
        delta_handled_request_count: int | None = None,
        new_handled_request_count: int | None = None,
        delta_pending_request_count: int | None = None,
        new_pending_request_count: int | None = None,
        delta_total_request_count: int | None = None,
        new_total_request_count: int | None = None,
        update_had_multiple_clients: bool = False,
    ) -> None:
        """Update the request queue metadata with current information.

        Args:
            pipeline: The Redis pipeline to use for the update.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
            new_handled_request_count: If provided, update the handled_request_count to this value.
            new_pending_request_count: If provided, update the pending_request_count to this value.
            new_total_request_count: If provided, update the total_request_count to this value.
            delta_handled_request_count: If provided, add this value to the handled_request_count.
            delta_pending_request_count: If provided, add this value to the pending_request_count.
            delta_total_request_count: If provided, add this value to the total_request_count.
            update_had_multiple_clients: If True, set had_multiple_clients to True.
        """
        now = datetime.now(timezone.utc)

        metadata_key = f'{self._MAIN_KEY}:{self._storage_name}:metadata'
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.accessed_at', now.isoformat(), nx=False, xx=True)
            )
        if update_modified_at:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.modified_at', now.isoformat(), nx=False, xx=True)
            )
        if new_pending_request_count is not None:
            await await_redis_response(
                pipeline.json().set(
                    metadata_key, '$.pending_request_count', new_pending_request_count, nx=False, xx=True
                )
            )
        elif delta_pending_request_count is not None:
            await await_redis_response(
                pipeline.json().numincrby(metadata_key, '$.pending_request_count', delta_pending_request_count)
            )

        if new_handled_request_count is not None:
            await await_redis_response(
                pipeline.json().set(
                    metadata_key, '$.handled_request_count', new_handled_request_count, nx=False, xx=True
                )
            )
        elif delta_handled_request_count is not None:
            await await_redis_response(
                pipeline.json().numincrby(metadata_key, '$.handled_request_count', delta_handled_request_count)
            )

        if new_total_request_count is not None:
            await await_redis_response(
                pipeline.json().set(metadata_key, '$.total_request_count', new_total_request_count, nx=False, xx=True)
            )
        elif delta_total_request_count is not None:
            await await_redis_response(
                pipeline.json().numincrby(metadata_key, '$.total_request_count', delta_total_request_count)
            )

        if update_had_multiple_clients:
            await await_redis_response(
                pipeline.json().set(
                    metadata_key, '$.had_multiple_clients', update_had_multiple_clients, nx=False, xx=True
                )
            )
