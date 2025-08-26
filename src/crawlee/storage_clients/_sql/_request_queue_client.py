from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from cachetools import LRUCache
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only
from typing_extensions import override

from crawlee import Request
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    ProcessedRequest,
    RequestQueueMetadata,
    UnprocessedRequest,
)

from ._client_mixin import SQLClientMixin
from ._db_models import RequestDB, RequestQueueMetadataDB, RequestQueueStateDB

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SQLStorageClient


logger = getLogger(__name__)


class SQLRequestQueueClient(RequestQueueClient, SQLClientMixin):
    """SQL implementation of the request queue client.

    This client persists requests to a SQL database with transaction handling and
    concurrent access safety. Requests are stored with sequence-based ordering and
    efficient querying capabilities.

    The implementation uses negative sequence numbers for forefront (high-priority) requests
    and positive sequence numbers for regular requests, allowing for efficient single-query
    ordering. A cache mechanism reduces database queries.

    The request queue data is stored in SQL database tables following the pattern:
    - `request_queue_metadata` table: Contains queue metadata (id, name, timestamps, request counts, multi-client flag)
    - `request` table: Contains individual requests with JSON data, unique keys for deduplication, sequence numbers for
        ordering, and processing status flags
    - `request_queue_state` table: Maintains counters for sequence numbers to ensure proper ordering of requests.

    Requests are serialized to JSON for storage and maintain proper ordering through sequence
    numbers. The implementation provides concurrent access safety through transaction
    handling, locking mechanisms, and optimized database indexes for efficient querying.
    """

    _DEFAULT_NAME = 'default'
    """Default dataset name used when no name is provided."""

    _MAX_BATCH_FETCH_SIZE = 10
    """Maximum number of requests to fetch from the database in a single batch operation.

    Used to limit the number of requests loaded and locked for processing at once (improves efficiency and reduces
    database load).
    """

    _METADATA_TABLE = RequestQueueMetadataDB
    """SQLAlchemy model for request queue metadata."""

    _ITEM_TABLE = RequestDB
    """SQLAlchemy model for request items."""

    _CLIENT_TYPE = 'Request queue'
    """Human-readable client type for error messages."""

    _REQUEST_ID_BY_KEY: LRUCache[str, int] = LRUCache(maxsize=10000)
    """Cache mapping unique keys to integer IDs."""

    _BLOCK_REQUEST_TIME = 300
    """Number of seconds for which a request is considered blocked in the database after being fetched for processing.
    """

    def __init__(
        self,
        *,
        id: str,
        storage_client: SQLStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SQLRequestQueueClient.open` class method to create a new instance.
        """
        super().__init__(id=id, storage_client=storage_client)

        self._pending_fetch_cache: deque[Request] = deque()
        """Cache for requests: ordered by sequence number."""

    async def _get_state(self, session: AsyncSession) -> RequestQueueStateDB:
        """Get the current state of the request queue."""
        orm_state: RequestQueueStateDB | None = await session.get(RequestQueueStateDB, self._id)
        if not orm_state:
            orm_state = RequestQueueStateDB(metadata_id=self._id)
            session.add(orm_state)
            await session.flush()
        return orm_state

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        """Get the metadata for this request queue."""
        # The database is a single place of truth
        metadata = await self._get_metadata(RequestQueueMetadata)
        return cast('RequestQueueMetadata', metadata)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_client: SQLStorageClient,
    ) -> SQLRequestQueueClient:
        """Open an existing request queue or create a new one.

        This method first tries to find an existing queue by ID or name.
        If found, it returns a client for that queue. If not found, it creates
        a new queue with the specified parameters.

        Args:
            id: The ID of the request queue to open. Takes precedence over name.
            name: The name of the request queue to open. Uses 'default' if None.
            storage_client: The SQL storage client used to access the database.

        Returns:
            An instance for the opened or created request queue.

        Raises:
            ValueError: If a queue with the specified ID is not found.
        """
        return await cls._safely_open(
            id=id,
            name=name,
            storage_client=storage_client,
            metadata_model=RequestQueueMetadata,
            extra_metadata_fields={
                'had_multiple_clients': False,
                'handled_request_count': 0,
                'pending_request_count': 0,
                'total_request_count': 0,
            },
        )

    @override
    async def drop(self) -> None:
        """Delete this request queue and all its records from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related records.
        """
        await self._drop()

        self._pending_fetch_cache.clear()

    @override
    async def purge(self) -> None:
        """Purge all requests from this request queue."""
        await self._purge(
            metadata_kwargs={
                'update_accessed_at': True,
                'update_modified_at': True,
                'new_pending_request_count': 0,
                'new_handled_request_count': 0,
                'force': True,
            }
        )

        # Clear recoverable state
        self._pending_fetch_cache.clear()

    async def _add_batch_of_requests_optimization(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        if not requests:
            return AddRequestsResponse(processed_requests=[], unprocessed_requests=[])

        # Clear empty cache since we're adding requests
        processed_requests = []
        unprocessed_requests = []

        metadata_recalculate = False

        # Deduplicate requests by unique_key upfront
        unique_requests = {}
        unique_key_by_request_id = {}
        for req in requests:
            if req.unique_key not in unique_requests:
                request_id = self._get_int_id_from_unique_key(req.unique_key)
                unique_requests[request_id] = req
                unique_key_by_request_id[request_id] = req.unique_key
                self._REQUEST_ID_BY_KEY[req.unique_key] = request_id

        # Get existing requests by unique keys
        stmt = (
            select(self._ITEM_TABLE)
            .where(
                self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.request_id.in_(set(unique_requests.keys()))
            )
            .options(
                load_only(
                    self._ITEM_TABLE.request_id,
                    self._ITEM_TABLE.is_handled,
                )
            )
        )

        async with self.get_session() as session:
            result = await session.execute(stmt)
            existing_requests = {req.request_id: req for req in result.scalars()}
            state = await self._get_state(session)
            insert_values: list[dict] = []

            for request_id, request in sorted(unique_requests.items()):
                existing_req_db = existing_requests.get(request_id)
                if existing_req_db is None or not existing_req_db.is_handled:
                    value = {
                        'request_id': request_id,
                        'metadata_id': self._id,
                        'data': request.model_dump_json(),
                        'is_handled': False,
                    }
                    if forefront:
                        value['sequence_number'] = state.forefront_sequence_counter
                        state.forefront_sequence_counter -= 1
                    else:
                        value['sequence_number'] = state.sequence_counter
                        state.sequence_counter += 1

                    insert_values.append(value)

                    if existing_req_db is None:
                        metadata_recalculate = True
                        processed_requests.append(
                            ProcessedRequest(
                                unique_key=request.unique_key,
                                was_already_present=False,
                                was_already_handled=False,
                            )
                        )
                    else:
                        processed_requests.append(
                            ProcessedRequest(
                                unique_key=request.unique_key,
                                was_already_present=True,
                                was_already_handled=existing_req_db.is_handled,
                            )
                        )

                else:
                    # Already handled request, skip adding
                    processed_requests.append(
                        ProcessedRequest(
                            unique_key=unique_key_by_request_id[request_id],
                            was_already_present=True,
                            was_already_handled=True,
                        )
                    )

            if insert_values:
                if forefront:
                    # If the request already exists in the database, we update the sequence_number by shifting request
                    # to the left.
                    upsert_stmt = self.build_upsert_stmt(
                        self._ITEM_TABLE,
                        insert_values,
                        update_columns=['sequence_number'],
                        conflict_cols=['request_id', 'metadata_id'],
                    )
                    await session.execute(upsert_stmt)
                else:
                    # If the request already exists in the database, we ignore this request when inserting.
                    insert_stmt_with_ignore = self.build_insert_stmt_with_ignore(self._ITEM_TABLE, insert_values)
                    await session.execute(insert_stmt_with_ignore)

            if metadata_recalculate:
                await self._block_metadata_for_update(session)

            await self._update_metadata(
                session,
                recalculate=metadata_recalculate,
                update_modified_at=True,
                update_accessed_at=True,
                force=metadata_recalculate,
            )

            try:
                await session.commit()
            except SQLAlchemyError as e:
                await session.rollback()
                logger.warning(f'Failed to commit session: {e}')
                await self._block_metadata_for_update(session)
                await self._update_metadata(
                    session, recalculate=True, update_modified_at=True, update_accessed_at=True, force=True
                )
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
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        if self._storage_client.get_dialect_name() in {'sqlite', 'postgresql'}:
            return await self._add_batch_of_requests_optimization(requests, forefront=forefront)

        raise NotImplementedError('Batch addition is not supported for this database dialect.')

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        if not (request_id := self._REQUEST_ID_BY_KEY.get(unique_key)):
            request_id = self._get_int_id_from_unique_key(unique_key)
            self._REQUEST_ID_BY_KEY[unique_key] = request_id

        stmt = select(self._ITEM_TABLE).where(
            self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.request_id == request_id
        )
        async with self.get_session() as session:
            result = await session.execute(stmt)
            request_db = result.scalar_one_or_none()

            if request_db is None:
                logger.warning(f'Request with ID "{unique_key}" not found in the queue.')
                return None

            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

        return Request.model_validate_json(request_db.data)

    @override
    async def fetch_next_request(self) -> Request | None:
        if self._pending_fetch_cache:
            return self._pending_fetch_cache.popleft()

        now = datetime.now(timezone.utc)
        block_until = now + timedelta(seconds=self._BLOCK_REQUEST_TIME)
        dialect = self._storage_client.get_dialect_name()

        # Get available requests not blocked by another client
        stmt = (
            select(self._ITEM_TABLE)
            .where(
                self._ITEM_TABLE.metadata_id == self._id,
                self._ITEM_TABLE.is_handled.is_(False),
                or_(self._ITEM_TABLE.time_blocked_until.is_(None), self._ITEM_TABLE.time_blocked_until < now),
            )
            .order_by(self._ITEM_TABLE.sequence_number.asc())
            .limit(self._MAX_BATCH_FETCH_SIZE)
        )

        # We use the `skip_locked` database mechanism to prevent the “interception” of requests by another client
        if dialect == 'postgresql':
            stmt = stmt.with_for_update(skip_locked=True)

        async with self.get_session() as session:
            result = await session.execute(stmt)
            requests_db = result.scalars().all()
            if not requests_db:
                return None

            request_ids = {r.request_id for r in requests_db}

            # Mark the requests as blocked
            update_stmt = (
                update(self._ITEM_TABLE)
                .where(
                    self._ITEM_TABLE.metadata_id == self._id,
                    self._ITEM_TABLE.request_id.in_(request_ids),
                    self._ITEM_TABLE.is_handled.is_(False),
                    or_(self._ITEM_TABLE.time_blocked_until.is_(None), self._ITEM_TABLE.time_blocked_until < now),
                )
                .values(time_blocked_until=block_until)
                .returning(self._ITEM_TABLE.request_id)
            )

            update_result = await session.execute(update_stmt)

            # Get IDs of successfully blocked requests
            blocked_ids = {row[0] for row in update_result.fetchall()}

            if not blocked_ids:
                await session.rollback()
                return None

            await self._update_metadata(session, update_accessed_at=True)

            await session.commit()

        requests = [Request.model_validate_json(r.data) for r in requests_db if r.request_id in blocked_ids]

        if not requests:
            return None

        self._pending_fetch_cache.extend(requests[1:])

        return requests[0]

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        if not (request_id := self._REQUEST_ID_BY_KEY.get(request.unique_key)):
            request_id = self._get_int_id_from_unique_key(request.unique_key)

        # Update the request's handled_at timestamp.
        if request.handled_at is None:
            request.handled_at = datetime.now(timezone.utc)

        # Update request in DB
        stmt = (
            update(self._ITEM_TABLE)
            .where(self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.request_id == request_id)
            .values(is_handled=True, time_blocked_until=None, data=request.model_dump_json())
        )
        async with self.get_session() as session:
            result = await session.execute(stmt)
            if result.rowcount == 0:
                logger.warning(f'Request {request.unique_key} not found in database.')
                return None
            await self._update_metadata(
                session,
                delta_handled_request_count=1,
                delta_pending_request_count=-1,
                update_modified_at=True,
                update_accessed_at=True,
                force=True,
            )
            await session.commit()
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
        if not (request_id := self._REQUEST_ID_BY_KEY.get(request.unique_key)):
            request_id = self._get_int_id_from_unique_key(request.unique_key)

        stmt = update(self._ITEM_TABLE).where(
            self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.request_id == request_id
        )

        async with self.get_autocommit_session() as autocommit:
            state = await self._get_state(autocommit)

            # Update sequence number if changing priority
            if forefront:
                new_sequence = state.forefront_sequence_counter
                state.forefront_sequence_counter -= 1
                now = datetime.now(timezone.utc)
                block_until = now + timedelta(seconds=self._BLOCK_REQUEST_TIME)
                # Extend blocking for forefront request, it is considered blocked by the current client.
                stmt = stmt.values(sequence_number=new_sequence, time_blocked_until=block_until)
            else:
                new_sequence = state.sequence_counter
                state.sequence_counter += 1
                stmt = stmt.values(sequence_number=new_sequence, time_blocked_until=None)

            result = await autocommit.execute(stmt)
            if result.rowcount == 0:
                logger.warning(f'Request {request.unique_key} not found in database.')
                return None
            await self._update_metadata(autocommit, update_modified_at=True, update_accessed_at=True)

        # put the forefront request at the beginning of the cache
        if forefront:
            self._pending_fetch_cache.appendleft(request)

        return ProcessedRequest(
            unique_key=request.unique_key,
            was_already_present=True,
            was_already_handled=False,
        )

    @override
    async def is_empty(self) -> bool:
        # Check in-memory cache for requests
        if self._pending_fetch_cache:
            return False

        # Check database for unhandled requests
        async with self.get_session() as session:
            metadata_orm = await session.get(self._METADATA_TABLE, self._id)
            if not metadata_orm:
                raise ValueError(f'Request queue with ID "{self._id}" not found.')

            empty = metadata_orm.pending_request_count == 0
            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

        return empty

    async def _block_metadata_for_update(self, session: AsyncSession) -> None:
        if self._storage_client.get_dialect_name() == 'postgresql':
            stmt = select(self._METADATA_TABLE).where(self._METADATA_TABLE.id == self._id).with_for_update()
            await session.execute(stmt)

    def _specific_update_metadata(
        self,
        new_handled_request_count: int | None = None,
        new_pending_request_count: int | None = None,
        new_total_request_count: int | None = None,
        delta_handled_request_count: int | None = None,
        delta_pending_request_count: int | None = None,
        *,
        recalculate: bool = False,
        update_had_multiple_clients: bool = False,
        **_kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        """Update the request queue metadata in the database.

        Args:
            session: The SQLAlchemy session to use for database operations.
            new_handled_request_count: If provided, update the handled_request_count to this value.
            new_pending_request_count: If provided, update the pending_request_count to this value.
            new_total_request_count: If provided, update the total_request_count to this value.
            delta_handled_request_count: If provided, add this value to the handled_request_count.
            delta_pending_request_count: If provided, add this value to the pending_request_count.
            recalculate: If True, recalculate the pending_request_count, and total_request_count on request table.
            update_had_multiple_clients: If True, set had_multiple_clients to True.
        """
        values_to_set: dict[str, Any] = {}

        if update_had_multiple_clients:
            values_to_set['had_multiple_clients'] = True

        if new_handled_request_count is not None:
            values_to_set['handled_request_count'] = new_handled_request_count
        elif delta_handled_request_count is not None:
            values_to_set['handled_request_count'] = (
                self._METADATA_TABLE.handled_request_count + delta_handled_request_count
            )

        if new_pending_request_count is not None:
            values_to_set['pending_request_count'] = new_pending_request_count
        elif delta_pending_request_count is not None:
            values_to_set['pending_request_count'] = (
                self._METADATA_TABLE.pending_request_count + delta_pending_request_count
            )

        if new_total_request_count is not None:
            values_to_set['total_request_count'] = new_total_request_count

        if recalculate:
            pending_count = (
                select(func.count())
                .select_from(self._ITEM_TABLE)
                .where(self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.is_handled.is_(False))
                .scalar_subquery()
            )

            total_count = (
                select(func.count())
                .select_from(self._ITEM_TABLE)
                .where(self._ITEM_TABLE.metadata_id == self._id)
                .scalar_subquery()
            )

            stmt = update(self._METADATA_TABLE).where(self._METADATA_TABLE.id == self._id)

            values_to_set['custom_stmt'] = stmt
            values_to_set['pending_request_count'] = pending_count
            values_to_set['total_request_count'] = total_count

        return values_to_set

    @staticmethod
    def _get_int_id_from_unique_key(unique_key: str) -> int:
        """Generate a deterministic integer ID for a unique_key.

        Args:
            unique_key: Unique key to be used to generate ID.

        Returns:
            An integer ID based on the unique_key.
        """
        hashed_key = sha256(unique_key.encode('utf-8')).hexdigest()
        name_length = 15
        return int(hashed_key[:name_length], 16)
