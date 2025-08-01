from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only
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
    from collections.abc import AsyncIterator, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SQLStorageClient


logger = getLogger(__name__)


class RequestQueueState(BaseModel):
    """Simplified state model for SQL implementation."""

    sequence_counter: int = 1
    """Counter for regular request ordering (positive)."""

    forefront_sequence_counter: int = -1
    """Counter for forefront request ordering (negative)."""


class SQLRequestQueueClient(RequestQueueClient):
    """SQL implementation of the request queue client.

    This client persists requests to a SQL database with proper transaction handling and
    concurrent access safety. Requests are stored in a normalized table structure with
    sequence-based ordering and efficient querying capabilities.

    The implementation uses negative sequence numbers for forefront (high-priority) requests
    and positive sequence numbers for regular requests, allowing for efficient single-query
    ordering. A cache mechanism reduces database queries for better performance.
    """

    _DEFAULT_NAME_DB = 'default'
    """Default dataset name used when no name is provided."""

    _MAX_REQUESTS_IN_CACHE = 100_000
    """Maximum number of requests to keep in cache for faster access."""

    def __init__(
        self,
        *,
        id: str,
        storage_client: SQLStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SQLRequestQueueClient.open` class method to create a new instance.
        """
        self._id = id

        self._request_cache: deque[Request] = deque()
        """Cache for requests: ordered by sequence number."""

        self.in_progress_requests: set[str] = set()
        """Set of request IDs currently being processed."""

        self._request_cache_needs_refresh = True
        """Flag indicating whether the cache needs to be refreshed from database."""

        self._is_empty_cache: bool | None = None
        """Cache for is_empty result: None means unknown, True/False is cached state."""

        self._state = RecoverableState[RequestQueueState](
            default_state=RequestQueueState(),
            persist_state_key='request_queue_state',
            persistence_enabled=True,
            persist_state_kvs_name=f'__RQ_STATE_{self._id}',
            logger=logger,
        )
        """Recoverable state to maintain request ordering and in-progress status."""

        self._storage_client = storage_client
        """The storage client used to access the SQL database."""

        self._lock = asyncio.Lock()

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        async with self.get_session() as session:
            orm_metadata: RequestQueueMetadataDB | None = await session.get(RequestQueueMetadataDB, self._id)
            if not orm_metadata:
                raise ValueError(f'Request queue with ID "{self._id}" not found.')

            return RequestQueueMetadata.model_validate(orm_metadata)

    def get_session(self) -> AsyncSession:
        """Create a new SQLAlchemy session for this key-value store."""
        return self._storage_client.create_session()

    @asynccontextmanager
    async def get_autocommit_session(self) -> AsyncIterator[AsyncSession]:
        """Create a new SQLAlchemy autocommit session to insert, delete, or modify data."""
        async with self.get_session() as session:
            try:
                yield session
                await session.commit()
            except SQLAlchemyError as e:
                logger.warning(f'Error occurred during session transaction: {e}')
                # Rollback the session in case of an error
                await session.rollback()

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
            orm_metadata: RequestQueueMetadataDB | None = None
            if id:
                orm_metadata = await session.get(RequestQueueMetadataDB, id)
                if not orm_metadata:
                    raise ValueError(f'Request queue with ID "{id}" not found.')
            else:
                # Try to find by name
                search_name = name or cls._DEFAULT_NAME_DB
                stmt = select(RequestQueueMetadataDB).where(RequestQueueMetadataDB.name == search_name)
                result = await session.execute(stmt)
                orm_metadata = result.scalar_one_or_none()
            if orm_metadata:
                client = cls(
                    id=orm_metadata.id,
                    storage_client=storage_client,
                )
                await client._update_metadata(session, update_accessed_at=True)
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

                client = cls(
                    id=metadata.id,
                    storage_client=storage_client,
                )
                session.add(RequestQueueMetadataDB(**metadata.model_dump()))

            try:
                # Commit the insert or update metadata to the database
                await session.commit()
            except SQLAlchemyError:
                # Attempt to open simultaneously by different clients.
                # The commit that created the record has already been executed, make rollback and get by name.
                await session.rollback()
                search_name = name or cls._DEFAULT_NAME_DB
                stmt = select(RequestQueueMetadataDB).where(RequestQueueMetadataDB.name == search_name)
                result = await session.execute(stmt)
                orm_metadata = result.scalar_one_or_none()
                if not orm_metadata:
                    raise ValueError(f'Request queue with Name "{search_name}" not found.') from None
                client = cls(
                    id=orm_metadata.id,
                    storage_client=storage_client,
                )

        await client._state.initialize()

        return client

    @override
    async def drop(self) -> None:
        stmt = delete(RequestQueueMetadataDB).where(RequestQueueMetadataDB.id == self._id)
        async with self.get_autocommit_session() as autocommit:
            if self._storage_client.get_default_flag():
                # foreign_keys=ON is set at the connection level. Required for cascade deletion.
                await autocommit.execute(text('PRAGMA foreign_keys=ON'))
            # Delete the request queue metadata (cascade will delete requests)
            await autocommit.execute(stmt)

        # Clear recoverable state
        await self._state.reset()
        await self._state.teardown()
        self._request_cache.clear()
        self._request_cache_needs_refresh = True
        self._is_empty_cache = None

    @override
    async def purge(self) -> None:
        stmt = delete(RequestDB).where(RequestDB.queue_id == self._id)
        async with self.get_autocommit_session() as autocommit:
            # Delete all requests for this queue
            await autocommit.execute(stmt)

            await self._update_metadata(
                autocommit,
                new_pending_request_count=0,
                new_handled_request_count=0,
                update_modified_at=True,
                update_accessed_at=True,
            )

        self._is_empty_cache = None

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
        if not requests:
            return AddRequestsResponse(processed_requests=[], unprocessed_requests=[])

        self._is_empty_cache = None
        processed_requests = []
        unprocessed_requests = []

        delta_total_request_count = 0
        delta_pending_request_count = 0

        # Deduplicate requests by unique_key upfront
        unique_requests = {}
        for req in requests:
            if req.unique_key not in unique_requests:
                unique_requests[req.unique_key] = req

        unique_keys = list(unique_requests.keys())

        # Get existing requests by unique keys
        stmt = (
            select(RequestDB)
            .where(RequestDB.queue_id == self._id, RequestDB.unique_key.in_(unique_keys))
            .options(
                load_only(
                    RequestDB.request_id,
                    RequestDB.unique_key,
                    RequestDB.is_handled,
                    RequestDB.sequence_number,
                )
            )
        )

        state = self._state.current_value

        async with self.get_session() as session, self._lock:
            result = await session.execute(stmt)
            existing_requests = {req.unique_key: req for req in result.scalars()}

            new_request_objects = []

            # Process each request
            for unique_key, request in unique_requests.items():
                existing_req_db = existing_requests.get(unique_key)

                # New request
                if existing_req_db is None:
                    if forefront:
                        sequence_number = state.forefront_sequence_counter
                        state.forefront_sequence_counter -= 1
                    else:
                        sequence_number = state.sequence_counter
                        state.sequence_counter += 1

                    new_request_objects.append(
                        RequestDB(
                            request_id=request.id,
                            queue_id=self._id,
                            data=request.model_dump_json(),
                            unique_key=request.unique_key,
                            sequence_number=sequence_number,
                            is_handled=False,
                        )
                    )

                    delta_total_request_count += 1
                    delta_pending_request_count += 1

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

                    processed_requests.append(
                        ProcessedRequest(
                            id=existing_req_db.request_id,
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=False,
                        )
                    )

            if new_request_objects:
                session.add_all(new_request_objects)

            await self._update_metadata(
                session,
                delta_total_request_count=delta_total_request_count,
                delta_pending_request_count=delta_pending_request_count,
                update_modified_at=True,
                update_accessed_at=True,
            )

            try:
                await session.commit()
            except SQLAlchemyError as e:
                logger.warning(f'Failed to commit session: {e}')
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

        if forefront:
            self._request_cache_needs_refresh = True

        return AddRequestsResponse(
            processed_requests=processed_requests,
            unprocessed_requests=unprocessed_requests,
        )

    @override
    async def get_request(self, request_id: str) -> Request | None:
        stmt = select(RequestDB).where(RequestDB.queue_id == self._id, RequestDB.request_id == request_id)
        async with self.get_session() as session:
            result = await session.execute(stmt)
            request_db = result.scalar_one_or_none()

            if request_db is None:
                logger.warning(f'Request with ID "{request_id}" not found in the queue.')
                return None

            await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            await session.commit()

        request = Request.model_validate_json(request_db.data)

        self.in_progress_requests.add(request.id)

        return request

    @override
    async def fetch_next_request(self) -> Request | None:
        # Refresh cache if needed
        async with self._lock:
            if self._request_cache_needs_refresh or not self._request_cache:
                await self._refresh_cache()

        next_request = None

        # Get from cache
        while self._request_cache and next_request is None:
            candidate = self._request_cache.popleft()

            # Only check local state
            if candidate.id not in self.in_progress_requests:
                next_request = candidate
                self.in_progress_requests.add(next_request.id)

        if not self._request_cache:
            self._is_empty_cache = None

        return next_request

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        self._is_empty_cache = None

        if request.id not in self.in_progress_requests:
            logger.warning(f'Marking request {request.id} as handled that is not in progress.')
            return None

        # Update request in DB
        stmt = (
            update(RequestDB)
            .where(RequestDB.queue_id == self._id, RequestDB.request_id == request.id)
            .values(is_handled=True)
        )

        async with self.get_session() as session:
            result = await session.execute(stmt)

            if result.rowcount == 0:
                logger.warning(f'Request {request.id} not found in database.')
                return None

            await self._update_metadata(
                session,
                delta_handled_request_count=1,
                delta_pending_request_count=-1,
                update_modified_at=True,
                update_accessed_at=True,
            )

            try:
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                return None

        self.in_progress_requests.discard(request.id)

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

        if request.id not in self.in_progress_requests:
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
            .where(RequestDB.queue_id == self._id, RequestDB.request_id == request.id)
            .values(sequence_number=new_sequence)
        )

        async with self.get_autocommit_session() as autocommit:
            result = await autocommit.execute(stmt)

            if result.rowcount == 0:
                logger.warning(f'Request {request.id} not found in database.')
                return None

            # Remove from in-progress
            self.in_progress_requests.discard(request.id)

            # Invalidate cache or add to cache
            if forefront:
                self._request_cache_needs_refresh = True
            elif len(self._request_cache) < self._MAX_REQUESTS_IN_CACHE:
                # For regular requests, we can add to the end if there's space
                self._request_cache.append(request)

            await self._update_metadata(autocommit, update_modified_at=True, update_accessed_at=True)

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

        # If there are in-progress requests, not empty
        if len(self.in_progress_requests) > 0:
            self._is_empty_cache = False
            return False

        # Check database for unhandled requests
        async with self.get_session() as session:
            metadata_orm = await session.get(RequestQueueMetadataDB, self._id)
            if not metadata_orm:
                raise ValueError(f'Request queue with ID "{self._id}" not found.')

            self._is_empty_cache = metadata_orm.pending_request_count == 0
            await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            await session.commit()

        return self._is_empty_cache

    async def _refresh_cache(self) -> None:
        """Refresh the request cache from database."""
        self._request_cache.clear()

        async with self.get_session() as session:
            # Simple query - get unhandled requests not in progress
            stmt = (
                select(RequestDB)
                .where(
                    RequestDB.queue_id == self._id,
                    RequestDB.is_handled == False,  # noqa: E712
                    RequestDB.request_id.notin_(self.in_progress_requests),
                )
                .order_by(RequestDB.sequence_number.asc())
                .limit(self._MAX_REQUESTS_IN_CACHE)
            )

            result = await session.execute(stmt)
            request_dbs = result.scalars().all()

        # Add to cache in order
        for request_db in request_dbs:
            request = Request.model_validate_json(request_db.data)
            self._request_cache.append(request)

        self._request_cache_needs_refresh = False

    async def _update_metadata(
        self,
        session: AsyncSession,
        *,
        new_handled_request_count: int | None = None,
        new_pending_request_count: int | None = None,
        new_total_request_count: int | None = None,
        delta_handled_request_count: int | None = None,
        delta_pending_request_count: int | None = None,
        delta_total_request_count: int | None = None,
        update_had_multiple_clients: bool = False,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the request queue metadata in the database.

        Args:
            session: The SQLAlchemy session to use for database operations.
            new_handled_request_count: If provided, update the handled_request_count to this value.
            new_pending_request_count: If provided, update the pending_request_count to this value.
            new_total_request_count: If provided, update the total_request_count to this value.
            delta_handled_request_count: If provided, add this value to the handled_request_count.
            delta_pending_request_count: If provided, add this value to the pending_request_count.
            delta_total_request_count: If provided, add this value to the total_request_count.
            update_had_multiple_clients: If True, set had_multiple_clients to True.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)
        values_to_set: dict[str, Any] = {}

        if update_accessed_at:
            values_to_set['accessed_at'] = now

        if update_modified_at:
            values_to_set['modified_at'] = now

        if update_had_multiple_clients:
            values_to_set['had_multiple_clients'] = True

        if new_handled_request_count is not None:
            values_to_set['handled_request_count'] = new_handled_request_count
        elif delta_handled_request_count is not None:
            values_to_set['handled_request_count'] = (
                RequestQueueMetadataDB.handled_request_count + delta_handled_request_count
            )

        if new_pending_request_count is not None:
            values_to_set['pending_request_count'] = new_pending_request_count
        elif delta_pending_request_count is not None:
            values_to_set['pending_request_count'] = (
                RequestQueueMetadataDB.pending_request_count + delta_pending_request_count
            )

        if new_total_request_count is not None:
            values_to_set['total_request_count'] = new_total_request_count
        elif delta_total_request_count is not None:
            values_to_set['total_request_count'] = (
                RequestQueueMetadataDB.total_request_count + delta_total_request_count
            )

        if values_to_set:
            stmt = update(RequestQueueMetadataDB).where(RequestQueueMetadataDB.id == self._id).values(**values_to_set)
            await session.execute(stmt)
