from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from typing_extensions import override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.recoverable_state import RecoverableState
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    ProcessedRequest,
    RequestQueueMetadata,
    UnprocessedRequest,
)

from ._db_models import RequestDB, RequestQueueMetadataDB

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ._storage_client import SQLStorageClient


logger = getLogger(__name__)


class RequestQueueState(BaseModel):
    """Simplified state model for SQL implementation."""

    sequence_counter: int = 1
    """Counter for regular request ordering (positive)."""

    forefront_sequence_counter: int = -1
    """Counter for forefront request ordering (negative)."""

    in_progress_requests: set[str] = set()
    """Set of request IDs currently being processed."""


class SQLRequestQueueClient(RequestQueueClient):
    """SQL implementation of the request queue client.

    This client persists requests to a SQL database with proper transaction handling and
    concurrent access safety. Requests are stored in a normalized table structure with
    sequence-based ordering and efficient querying capabilities.

    The implementation uses negative sequence numbers for forefront (high-priority) requests
    and positive sequence numbers for regular requests, allowing for efficient single-query
    ordering. A cache mechanism reduces database queries for better performance.
    """

    _MAX_REQUESTS_IN_CACHE = 100_000
    """Maximum number of requests to keep in cache for faster access."""

    def __init__(
        self,
        *,
        orm_metadata: RequestQueueMetadataDB,
        storage_client: SQLStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SQLRequestQueueClient.open` class method to create a new instance.
        """
        self._orm_metadata = orm_metadata

        self._request_cache: deque[Request] = deque()
        """Cache for requests: ordered by sequence number."""

        self._request_cache_needs_refresh = True
        """Flag indicating whether the cache needs to be refreshed from database."""

        self._is_empty_cache: bool | None = None
        """Cache for is_empty result: None means unknown, True/False is cached state."""

        self._state = RecoverableState[RequestQueueState](
            default_state=RequestQueueState(),
            persist_state_key='request_queue_state',
            persistence_enabled=True,
            persist_state_kvs_name=f'__RQ_STATE_{self._orm_metadata.id}',
            logger=logger,
        )
        """Recoverable state to maintain request ordering and in-progress status."""

        self._storage_client = storage_client
        """The storage client used to access the SQL database."""

        self._lock = asyncio.Lock()

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        return RequestQueueMetadata.model_validate(self._orm_metadata)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_client: SQLStorageClient,
    ) -> SQLRequestQueueClient:
        """Open or create a SQL request queue client.

        Args:
            id: The ID of the request queue to open. If provided, searches for existing queue by ID.
            name: The name of the request queue to open. If not provided, uses the default queue.
            storage_client: The SQL storage client used to access the database.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a queue with the specified ID is not found.
        """
        async with storage_client.create_session() as session:
            if id:
                orm_metadata = await session.get(RequestQueueMetadataDB, id)
                if not orm_metadata:
                    raise ValueError(f'Request queue with ID "{id}" not found.')
                client = cls(
                    orm_metadata=orm_metadata,
                    storage_client=storage_client,
                )
                await client._update_metadata(update_accessed_at=True)
            else:
                # Try to find by name
                orm_metadata = await session.get(RequestQueueMetadataDB, name)

                if orm_metadata:
                    client = cls(
                        orm_metadata=orm_metadata,
                        storage_client=storage_client,
                    )
                    await client._update_metadata(update_accessed_at=True)
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
                        stats={},
                        total_request_count=0,
                    )
                    orm_metadata = RequestQueueMetadataDB(**metadata.model_dump())
                    client = cls(
                        orm_metadata=orm_metadata,
                        storage_client=storage_client,
                    )

                    session.add(orm_metadata)

            await session.commit()

        await client._state.initialize()

        return client

    @override
    async def drop(self) -> None:
        async with self._storage_client.create_session() as session:
            # Delete the request queue metadata (cascade will delete requests)
            rq_db = await session.get(RequestQueueMetadataDB, self._orm_metadata.id)
            if rq_db:
                await session.delete(rq_db)

            # Clear recoverable state
            await self._state.reset()
            await self._state.teardown()
            self._request_cache.clear()
            self._request_cache_needs_refresh = True
            self._is_empty_cache = None

            await session.commit()

    @override
    async def purge(self) -> None:
        async with self._storage_client.create_session() as session:
            # Delete all requests for this queue
            stmt = delete(RequestDB).where(RequestDB.queue_id == self._orm_metadata.id)
            await session.execute(stmt)

            # Update metadata
            self._orm_metadata.pending_request_count = 0
            self._orm_metadata.handled_request_count = 0

            await self._update_metadata(update_modified_at=True, update_accessed_at=True)

            self._is_empty_cache = None
            await session.commit()

        # Clear recoverable state
        self._request_cache.clear()
        self._request_cache_needs_refresh = True
        await self._state.reset()

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        async with self._storage_client.create_session() as session, self._lock:
            self._is_empty_cache = None
            processed_requests = []
            unprocessed_requests = []
            state = self._state.current_value

            # Get existing requests by unique keys
            unique_keys = {req.unique_key for req in requests}
            stmt = select(RequestDB).where(
                RequestDB.queue_id == self._orm_metadata.id, RequestDB.unique_key.in_(unique_keys)
            )
            result = await session.execute(stmt)
            existing_requests = {req.unique_key: req for req in result.scalars()}
            result = await session.execute(stmt)

            batch_processed = set()

            # Process each request
            for request in requests:
                if request.unique_key in batch_processed:
                    continue

                existing_req_db = existing_requests.get(request.unique_key)

                if existing_req_db is None:
                    # New request
                    if forefront:
                        sequence_number = state.forefront_sequence_counter
                        state.forefront_sequence_counter -= 1
                    else:
                        sequence_number = state.sequence_counter
                        state.sequence_counter += 1

                    request_db = RequestDB(
                        request_id=request.id,
                        queue_id=self._orm_metadata.id,
                        data=request.model_dump_json(),
                        unique_key=request.unique_key,
                        sequence_number=sequence_number,
                        is_handled=False,
                    )
                    session.add(request_db)

                    self._orm_metadata.total_request_count += 1
                    self._orm_metadata.pending_request_count += 1

                    processed_requests.append(
                        ProcessedRequest(
                            id=request.id,
                            unique_key=request.unique_key,
                            was_already_present=False,
                            was_already_handled=False,
                        )
                    )

                elif existing_req_db.is_handled:
                    # Already handled
                    processed_requests.append(
                        ProcessedRequest(
                            id=existing_req_db.request_id,
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=True,
                        )
                    )

                else:
                    # Exists but not handled - might update priority
                    if forefront and existing_req_db.sequence_number > 0:
                        existing_req_db.sequence_number = state.forefront_sequence_counter
                        state.forefront_sequence_counter -= 1
                        self._request_cache_needs_refresh = True

                    processed_requests.append(
                        ProcessedRequest(
                            id=existing_req_db.request_id,
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=False,
                        )
                    )

                batch_processed.add(request.unique_key)

            await self._update_metadata(update_modified_at=True, update_accessed_at=True)

            if forefront:
                self._request_cache_needs_refresh = True

            try:
                await session.commit()
            except SQLAlchemyError as e:
                logger.warning(f'Failed to commit session: {e}')
                await session.rollback()
                input()
                processed_requests.clear()
                unprocessed_requests.extend(
                    [
                        UnprocessedRequest(
                            unique_key=request.unique_key,
                            url=request.url,
                            method=request.method,
                        )
                        for request in requests
                        if request.unique_key not in existing_requests
                    ]
                )

            return AddRequestsResponse(
                processed_requests=processed_requests,
                unprocessed_requests=unprocessed_requests,
            )

    @override
    async def get_request(self, request_id: str) -> Request | None:
        async with self._storage_client.create_session() as session:
            stmt = select(RequestDB).where(
                RequestDB.queue_id == self._orm_metadata.id, RequestDB.request_id == request_id
            )
            result = await session.execute(stmt)
            request_db = result.scalar_one_or_none()

            if request_db is None:
                logger.warning(f'Request with ID "{request_id}" not found in the queue.')
                return None

            request = Request.model_validate_json(request_db.data)

            state = self._state.current_value
            state.in_progress_requests.add(request.id)

            await self._update_metadata(update_accessed_at=True)
            await session.commit()

            return request

    @override
    async def fetch_next_request(self) -> Request | None:
        # Refresh cache if needed
        if self._request_cache_needs_refresh or not self._request_cache:
            await self._refresh_cache()

        next_request = None
        state = self._state.current_value

        # Get from cache
        while self._request_cache and next_request is None:
            candidate = self._request_cache.popleft()

            # Only check local state
            if candidate.id not in state.in_progress_requests:
                next_request = candidate
                state.in_progress_requests.add(next_request.id)

        if not self._request_cache:
            self._is_empty_cache = None

        return next_request

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        self._is_empty_cache = None
        state = self._state.current_value

        if request.id not in state.in_progress_requests:
            logger.warning(f'Marking request {request.id} as handled that is not in progress.')
            return None

        # Update request in DB
        stmt = (
            update(RequestDB)
            .where(RequestDB.queue_id == self._orm_metadata.id, RequestDB.request_id == request.id)
            .values(is_handled=True)
        )

        async with self._storage_client.create_session() as session:
            result = await session.execute(stmt)

            if result.rowcount == 0:
                logger.warning(f'Request {request.id} not found in database.')
                return None

            # Update state
            state.in_progress_requests.discard(request.id)

            # Update metadata
            self._orm_metadata.handled_request_count += 1
            self._orm_metadata.pending_request_count -= 1

            await self._update_metadata(update_modified_at=True, update_accessed_at=True)

            await session.commit()

        return ProcessedRequest(
            id=request.id,
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
        self._is_empty_cache = None
        state = self._state.current_value

        if request.id not in state.in_progress_requests:
            logger.info(f'Reclaiming request {request.id} that is not in progress.')
            return None

        # Update sequence number if changing priority
        if forefront:
            new_sequence = state.forefront_sequence_counter
            state.forefront_sequence_counter -= 1
        else:
            new_sequence = state.sequence_counter
            state.sequence_counter += 1

        stmt = (
            update(RequestDB)
            .where(RequestDB.queue_id == self._orm_metadata.id, RequestDB.request_id == request.id)
            .values(sequence_number=new_sequence)
        )

        async with self._storage_client.create_session() as session:
            result = await session.execute(stmt)

            if result.rowcount == 0:
                logger.warning(f'Request {request.id} not found in database.')
                return None

            # Remove from in-progress
            state.in_progress_requests.discard(request.id)

            # Invalidate cache or add to cache
            if forefront:
                self._request_cache_needs_refresh = True
            elif len(self._request_cache) < self._MAX_REQUESTS_IN_CACHE:
                # For regular requests, we can add to the end if there's space
                self._request_cache.append(request)

            await self._update_metadata(update_modified_at=True, update_accessed_at=True)

            await session.commit()

            return ProcessedRequest(
                id=request.id,
                unique_key=request.unique_key,
                was_already_present=True,
                was_already_handled=False,
            )

    @override
    async def is_empty(self) -> bool:
        if self._is_empty_cache is not None:
            return self._is_empty_cache

        state = self._state.current_value

        # If there are in-progress requests, not empty
        if len(state.in_progress_requests) > 0:
            self._is_empty_cache = False
            return False

        # Check database for unhandled requests
        async with self._storage_client.create_session() as session:
            stmt = (
                select(func.count())
                .select_from(RequestDB)
                .where(
                    RequestDB.queue_id == self._orm_metadata.id,
                    RequestDB.is_handled == False,  # noqa: E712
                )
            )
            result = await session.execute(stmt)
            unhandled_count = result.scalar()
            self._is_empty_cache = unhandled_count == 0
            return self._is_empty_cache

    async def _refresh_cache(self) -> None:
        """Refresh the request cache from database."""
        self._request_cache.clear()
        state = self._state.current_value

        async with self._storage_client.create_session() as session:
            # Simple query - get unhandled requests not in progress
            stmt = (
                select(RequestDB)
                .where(
                    RequestDB.queue_id == self._orm_metadata.id,
                    RequestDB.is_handled == False,  # noqa: E712
                )
                .order_by(RequestDB.sequence_number.asc())
                .limit(self._MAX_REQUESTS_IN_CACHE)
            )

            if state.in_progress_requests:
                stmt = stmt.where(RequestDB.request_id.notin_(state.in_progress_requests))

            result = await session.execute(stmt)
            request_dbs = result.scalars().all()

        # Add to cache in order
        for request_db in request_dbs:
            request = Request.model_validate_json(request_db.data)
            self._request_cache.append(request)

        self._request_cache_needs_refresh = False

    async def _update_metadata(
        self,
        *,
        update_had_multiple_clients: bool = False,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the request queue metadata in the database.

        Args:
            session: The SQLAlchemy session to use for database operations.
            update_had_multiple_clients: If True, set had_multiple_clients to True.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._orm_metadata.accessed_at = now

        if update_modified_at:
            self._orm_metadata.modified_at = now

        if update_had_multiple_clients:
            self._orm_metadata.had_multiple_clients = True
