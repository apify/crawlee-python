from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from hashlib import sha256
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import CursorResult, func, or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only
from typing_extensions import NotRequired, Self, override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    ProcessedRequest,
    RequestQueueMetadata,
    UnprocessedRequest,
)

from ._client_mixin import MetadataUpdateParams, SqlClientMixin
from ._db_models import RequestDb, RequestQueueMetadataDb, RequestQueueStateDb

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SqlStorageClient


logger = getLogger(__name__)


class _QueueMetadataUpdateParams(MetadataUpdateParams):
    """Parameters for updating queue metadata."""

    new_handled_request_count: NotRequired[int]
    new_pending_request_count: NotRequired[int]
    new_total_request_count: NotRequired[int]
    delta_handled_request_count: NotRequired[int]
    delta_pending_request_count: NotRequired[int]
    recalculate: NotRequired[bool]
    update_had_multiple_clients: NotRequired[bool]


class SqlRequestQueueClient(RequestQueueClient, SqlClientMixin):
    """SQL implementation of the request queue client.

    This client persists requests to a SQL database with transaction handling and
    concurrent access safety. Requests are stored with sequence-based ordering and
    efficient querying capabilities.

    The implementation uses negative sequence numbers for forefront (high-priority) requests
    and positive sequence numbers for regular requests, allowing for efficient single-query
    ordering. A cache mechanism reduces database queries.

    The request queue data is stored in SQL database tables following the pattern:
    - `request_queues` table: Contains queue metadata (id, name, timestamps, request counts, multi-client flag)
    - `request_queue_records` table: Contains individual requests with JSON data, unique keys for deduplication,
    sequence numbers for ordering, and processing status flags
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

    _METADATA_TABLE = RequestQueueMetadataDb
    """SQLAlchemy model for request queue metadata."""

    _ITEM_TABLE = RequestDb
    """SQLAlchemy model for request items."""

    _CLIENT_TYPE = 'Request queue'
    """Human-readable client type for error messages."""

    _BLOCK_REQUEST_TIME = 300
    """Number of seconds for which a request is considered blocked in the database after being fetched for processing.
    """

    def __init__(
        self,
        *,
        id: str,
        storage_client: SqlStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SqlRequestQueueClient.open` class method to create a new instance.
        """
        super().__init__(id=id, storage_client=storage_client)

        self._pending_fetch_cache: deque[Request] = deque()
        """Cache for requests: ordered by sequence number."""

        self.client_key = crypto_random_object_id(length=32)[:32]
        """Unique identifier for this client instance."""

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        storage_client: SqlStorageClient,
    ) -> Self:
        """Open an existing request queue or create a new one.

        This method first tries to find an existing queue by ID or name.
        If found, it returns a client for that queue. If not found, it creates
        a new queue with the specified parameters.

        Args:
            id: The ID of the request queue to open. Takes precedence over name.
            name: The name of the request queue for named (global scope) storages.
            alias: The alias of the request queue for unnamed (run scope) storages.
            storage_client: The SQL storage client used to access the database.

        Returns:
            An instance for the opened or created request queue.

        Raises:
            ValueError: If a queue with the specified ID is not found.
        """
        return await cls._safely_open(
            id=id,
            name=name,
            alias=alias,
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
    async def get_metadata(self) -> RequestQueueMetadata:
        # The database is a single place of truth
        return await self._get_metadata(RequestQueueMetadata)

    @override
    async def drop(self) -> None:
        """Delete this request queue and all its records from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related records.
        """
        await self._drop()

        self._pending_fetch_cache.clear()

    @override
    async def purge(self) -> None:
        """Remove all items from this dataset while keeping the dataset structure.

        Resets pending_request_count and handled_request_count to 0 and deletes all records from request_queue_records
        table.
        """
        await self._purge(
            metadata_kwargs=_QueueMetadataUpdateParams(
                update_accessed_at=True,
                update_modified_at=True,
                new_pending_request_count=0,
                force=True,
            )
        )

        # Clear recoverable state
        self._pending_fetch_cache.clear()

    @override
    async def add_batch_of_requests(
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
        transaction_processed_requests = []
        transaction_processed_requests_unique_keys = set()

        metadata_recalculate = False

        # Deduplicate requests by unique_key upfront
        unique_requests = {}
        unique_key_by_request_id = {}
        for req in requests:
            if req.unique_key not in unique_requests:
                request_id = self._get_int_id_from_unique_key(req.unique_key)
                unique_requests[request_id] = req
                unique_key_by_request_id[request_id] = req.unique_key

        # Get existing requests by unique keys
        stmt = (
            select(self._ITEM_TABLE)
            .where(
                self._ITEM_TABLE.request_queue_id == self._id,
                self._ITEM_TABLE.request_id.in_(set(unique_requests.keys())),
            )
            .options(
                load_only(
                    self._ITEM_TABLE.request_id,
                    self._ITEM_TABLE.is_handled,
                    self._ITEM_TABLE.time_blocked_until,
                )
            )
        )

        async with self.get_session() as session:
            result = await session.execute(stmt)
            result = cast('CursorResult', result) if not isinstance(result, CursorResult) else result
            existing_requests = {req.request_id: req for req in result.scalars()}
            state = await self._get_state(session)
            insert_values: list[dict] = []

            for request_id, request in sorted(unique_requests.items()):
                existing_req_db = existing_requests.get(request_id)
                # New Request, add it
                if existing_req_db is None:
                    value = {
                        'request_id': request_id,
                        'request_queue_id': self._id,
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
                    metadata_recalculate = True
                    transaction_processed_requests.append(
                        ProcessedRequest(
                            unique_key=request.unique_key,
                            was_already_present=False,
                            was_already_handled=False,
                        )
                    )
                    transaction_processed_requests_unique_keys.add(request.unique_key)
                # Already handled request, skip adding
                elif existing_req_db and existing_req_db.is_handled:
                    processed_requests.append(
                        ProcessedRequest(
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=True,
                        )
                    )
                # Already in progress in one of the clients
                elif existing_req_db and existing_req_db.time_blocked_until:
                    processed_requests.append(
                        ProcessedRequest(
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=False,
                        )
                    )
                # Request in database but not yet handled and not in progress
                elif existing_req_db and not existing_req_db.is_handled and not existing_req_db.time_blocked_until:
                    # Forefront request, update its sequence number
                    if forefront:
                        insert_values.append(
                            {
                                'request_queue_id': self._id,
                                'request_id': request_id,
                                'sequence_number': state.forefront_sequence_counter,
                                'data': request.model_dump_json(),
                                'is_handled': False,
                            }
                        )
                        state.forefront_sequence_counter -= 1
                        transaction_processed_requests.append(
                            ProcessedRequest(
                                unique_key=request.unique_key,
                                was_already_present=True,
                                was_already_handled=False,
                            )
                        )
                        transaction_processed_requests_unique_keys.add(request.unique_key)
                    # Regular request, keep its position
                    else:
                        processed_requests.append(
                            ProcessedRequest(
                                unique_key=request.unique_key,
                                was_already_present=True,
                                was_already_handled=False,
                            )
                        )
                # Unexpected condition
                else:
                    unprocessed_requests.append(
                        UnprocessedRequest(
                            unique_key=request.unique_key,
                            url=request.url,
                            method=request.method,
                        )
                    )

            if insert_values:
                if forefront:
                    # If the request already exists in the database, we update the sequence_number by shifting request
                    # to the left.
                    upsert_stmt = self._build_upsert_stmt(
                        self._ITEM_TABLE,
                        insert_values,
                        update_columns=['sequence_number'],
                        conflict_cols=['request_id', 'request_queue_id'],
                    )
                    await session.execute(upsert_stmt)
                else:
                    # If the request already exists in the database, we ignore this request when inserting.
                    insert_stmt_with_ignore = self._build_insert_stmt_with_ignore(self._ITEM_TABLE, insert_values)
                    await session.execute(insert_stmt_with_ignore)

            await self._update_metadata(
                session,
                **_QueueMetadataUpdateParams(
                    recalculate=metadata_recalculate,
                    update_modified_at=True,
                    update_accessed_at=True,
                    force=metadata_recalculate,
                ),
            )

            try:
                await session.commit()
                processed_requests.extend(transaction_processed_requests)
            except SQLAlchemyError as e:
                await session.rollback()
                logger.warning(f'Failed to commit session: {e}')
                await self._update_metadata(
                    session, recalculate=True, update_modified_at=True, update_accessed_at=True, force=True
                )
                await session.commit()
                transaction_processed_requests.clear()
                unprocessed_requests.extend(
                    [
                        UnprocessedRequest(
                            unique_key=request.unique_key,
                            url=request.url,
                            method=request.method,
                        )
                        for request in requests
                        if request.unique_key in transaction_processed_requests_unique_keys
                    ]
                )

        return AddRequestsResponse(
            processed_requests=processed_requests,
            unprocessed_requests=unprocessed_requests,
        )

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        request_id = self._get_int_id_from_unique_key(unique_key)

        stmt = select(self._ITEM_TABLE).where(
            self._ITEM_TABLE.request_queue_id == self._id, self._ITEM_TABLE.request_id == request_id
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
                self._ITEM_TABLE.request_queue_id == self._id,
                self._ITEM_TABLE.is_handled.is_(False),
                or_(self._ITEM_TABLE.time_blocked_until.is_(None), self._ITEM_TABLE.time_blocked_until < now),
            )
            .order_by(self._ITEM_TABLE.sequence_number.asc())
            .limit(self._MAX_BATCH_FETCH_SIZE)
        )

        async with self.get_session() as session:
            # We use the `skip_locked` database mechanism to prevent the 'interception' of requests by another client
            if dialect == 'postgresql':
                stmt = stmt.with_for_update(skip_locked=True)
                result = await session.execute(stmt)
                requests_db = result.scalars().all()

                if not requests_db:
                    return None

                # All requests received have already been reserved for update with the help of `skip_locked`.
                request_ids = {r.request_id for r in requests_db}

                update_stmt = (
                    update(self._ITEM_TABLE)
                    .where(self._ITEM_TABLE.request_id.in_(request_ids))
                    .values(time_blocked_until=block_until, client_key=self.client_key)
                )
                await session.execute(update_stmt)

                blocked_ids = request_ids
            else:
                # For other databases, we first select the requests, then try to update them to be blocked.
                result = await session.execute(stmt)
                requests_db = result.scalars().all()

                if not requests_db:
                    return None

                request_ids = {r.request_id for r in requests_db}

                update_stmt = (
                    update(self._ITEM_TABLE)
                    .where(
                        self._ITEM_TABLE.request_queue_id == self._id,
                        self._ITEM_TABLE.request_id.in_(request_ids),
                        self._ITEM_TABLE.is_handled.is_(False),
                        or_(self._ITEM_TABLE.time_blocked_until.is_(None), self._ITEM_TABLE.time_blocked_until < now),
                    )
                    .values(time_blocked_until=block_until, client_key=self.client_key)
                    .returning(self._ITEM_TABLE.request_id)
                )

                update_result = await session.execute(update_stmt)
                blocked_ids = {row[0] for row in update_result.fetchall()}

                if not blocked_ids:
                    await session.rollback()
                    return None

            await self._update_metadata(session, **_QueueMetadataUpdateParams(update_accessed_at=True))

            await session.commit()

        requests = [Request.model_validate_json(r.data) for r in requests_db if r.request_id in blocked_ids]

        if not requests:
            return None

        self._pending_fetch_cache.extend(requests[1:])

        return requests[0]

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        request_id = self._get_int_id_from_unique_key(request.unique_key)

        # Update the request's handled_at timestamp.
        if request.handled_at is None:
            request.handled_at = datetime.now(timezone.utc)

        # Update request in Db
        stmt = (
            update(self._ITEM_TABLE)
            .where(self._ITEM_TABLE.request_queue_id == self._id, self._ITEM_TABLE.request_id == request_id)
            .values(is_handled=True, time_blocked_until=None, client_key=None, data=request.model_dump_json())
        )
        async with self.get_session() as session:
            result = await session.execute(stmt)
            result = cast('CursorResult', result) if not isinstance(result, CursorResult) else result

            if result.rowcount == 0:
                logger.warning(f'Request {request.unique_key} not found in database.')
                return None

            await self._update_metadata(
                session,
                **_QueueMetadataUpdateParams(
                    delta_handled_request_count=1,
                    delta_pending_request_count=-1,
                    update_modified_at=True,
                    update_accessed_at=True,
                    force=True,
                ),
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
        request_id = self._get_int_id_from_unique_key(request.unique_key)

        stmt = update(self._ITEM_TABLE).where(
            self._ITEM_TABLE.request_queue_id == self._id, self._ITEM_TABLE.request_id == request_id
        )

        async with self.get_session(with_simple_commit=True) as session:
            state = await self._get_state(session)

            # Update sequence number if changing priority
            if forefront:
                new_sequence = state.forefront_sequence_counter
                state.forefront_sequence_counter -= 1
                now = datetime.now(timezone.utc)
                block_until = now + timedelta(seconds=self._BLOCK_REQUEST_TIME)
                # Extend blocking for forefront request, it is considered blocked by the current client.
                stmt = stmt.values(
                    sequence_number=new_sequence,
                    time_blocked_until=block_until,
                    client_key=self.client_key,
                    data=request.model_dump_json(),
                )
            else:
                new_sequence = state.sequence_counter
                state.sequence_counter += 1
                stmt = stmt.values(
                    sequence_number=new_sequence,
                    time_blocked_until=None,
                    client_key=None,
                    data=request.model_dump_json(),
                )

            result = await session.execute(stmt)
            result = cast('CursorResult', result) if not isinstance(result, CursorResult) else result

            if result.rowcount == 0:
                logger.warning(f'Request {request.unique_key} not found in database.')
                return None
            await self._update_metadata(
                session, **_QueueMetadataUpdateParams(update_modified_at=True, update_accessed_at=True)
            )

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

            updated = await self._update_metadata(
                session,
                **_QueueMetadataUpdateParams(
                    update_accessed_at=True,
                    # With multi-client access, counters may become out of sync.
                    # If the queue is not empty, we perform a recalculation to synchronize the counters in the metadata.
                    recalculate=not empty,
                    update_modified_at=not empty,
                ),
            )

            # Commit updates to the metadata
            if updated:
                await session.commit()

        return empty

    async def _get_state(self, session: AsyncSession) -> RequestQueueStateDb:
        """Get the current state of the request queue."""
        orm_state: RequestQueueStateDb | None = await session.get(RequestQueueStateDb, self._id)
        if not orm_state:
            insert_values = {'request_queue_id': self._id}
            # Create a new state if it doesn't exist
            # This is a safeguard against race conditions where multiple clients might try to create the state
            # simultaneously.
            insert_stmt = self._build_insert_stmt_with_ignore(RequestQueueStateDb, insert_values)
            await session.execute(insert_stmt)
            await session.flush()
            orm_state = await session.get(RequestQueueStateDb, self._id)
            if not orm_state:
                raise RuntimeError(f'Failed to create or retrieve state for queue {self._id}')
        return orm_state

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
            stmt = (
                update(self._METADATA_TABLE)
                .where(self._METADATA_TABLE.request_queue_id == self._id)
                .values(
                    pending_request_count=(
                        select(func.count())
                        .select_from(self._ITEM_TABLE)
                        .where(self._ITEM_TABLE.request_queue_id == self._id, self._ITEM_TABLE.is_handled.is_(False))
                        .scalar_subquery()
                    ),
                    total_request_count=(
                        select(func.count())
                        .select_from(self._ITEM_TABLE)
                        .where(self._ITEM_TABLE.request_queue_id == self._id)
                        .scalar_subquery()
                    ),
                    handled_request_count=(
                        select(func.count())
                        .select_from(self._ITEM_TABLE)
                        .where(self._ITEM_TABLE.request_queue_id == self._id, self._ITEM_TABLE.is_handled.is_(True))
                        .scalar_subquery()
                    ),
                )
            )

            values_to_set['custom_stmt'] = stmt

        return values_to_set

    @staticmethod
    @lru_cache(maxsize=10000)
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
