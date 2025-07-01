from __future__ import annotations

import asyncio
import json
import shutil
from collections import deque
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError
from typing_extensions import override

from crawlee import Request
from crawlee._consts import METADATA_FILENAME
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import atomic_write, json_dumps
from crawlee._utils.recoverable_state import RecoverableState
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    ProcessedRequest,
    RequestQueueMetadata,
    UnprocessedRequest,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class RequestQueueState(BaseModel):
    """State model for the `FileSystemRequestQueueClient`."""

    sequence_counter: int = 0
    """Counter for regular request ordering."""

    forefront_sequence_counter: int = 0
    """Counter for forefront request ordering."""

    forefront_requests: dict[str, int] = {}
    """Mapping of forefront request IDs to their sequence numbers."""

    regular_requests: dict[str, int] = {}
    """Mapping of regular request IDs to their sequence numbers."""

    in_progress_requests: set[str] = set()
    """Set of request IDs currently being processed."""

    handled_requests: set[str] = set()
    """Set of request IDs that have been handled."""


class FileSystemRequestQueueClient(RequestQueueClient):
    """A file system implementation of the request queue client.

    This client persists requests to the file system as individual JSON files, making it suitable for scenarios
    where data needs to survive process restarts. Each request is stored as a separate file in a directory
    structure following the pattern:

    ```
    {STORAGE_DIR}/request_queues/{QUEUE_ID}/{REQUEST_ID}.json
    ```

    The implementation uses `RecoverableState` to maintain ordering information, in-progress status, and
    request handling status. This allows for proper state recovery across process restarts without
    embedding metadata in individual request files. File system storage provides durability at the cost of
    slower I/O operations compared to memory only-based storage.

    This implementation is ideal for long-running crawlers where persistence is important and for situations
    where you need to resume crawling after process termination.
    """

    _STORAGE_SUBDIR = 'request_queues'
    """The name of the subdirectory where request queues are stored."""

    _STORAGE_SUBSUBDIR_DEFAULT = 'default'
    """The name of the subdirectory for the default request queue."""

    _MAX_REQUESTS_IN_CACHE = 100_000
    """Maximum number of requests to keep in cache for faster access."""

    def __init__(
        self,
        *,
        metadata: RequestQueueMetadata,
        storage_dir: Path,
        lock: asyncio.Lock,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemRequestQueueClient.open` class method to create a new instance.
        """
        self._metadata = metadata

        self._storage_dir = storage_dir
        """The base directory where the storage data are being persisted."""

        self._lock = lock
        """A lock to ensure that only one operation is performed at a time."""

        self._request_cache = deque[Request]()
        """Cache for requests: forefront requests at the beginning, regular requests at the end."""

        self._request_cache_needs_refresh = True
        """Flag indicating whether the cache needs to be refreshed from filesystem."""

        self._is_empty_cache: bool | None = None
        """Cache for is_empty result: None means unknown, True/False is cached state."""

        self._state = RecoverableState[RequestQueueState](
            default_state=RequestQueueState(),
            persist_state_key='request_queue_state',
            persistence_enabled=True,
            persist_state_kvs_name=f'__RQ_STATE_{self._metadata.id}',
            logger=logger,
        )
        """Recoverable state to maintain request ordering, in-progress status, and handled status."""

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        return self._metadata

    @property
    def path_to_rq(self) -> Path:
        """The full path to the request queue directory."""
        if self._metadata.name is None:
            return self._storage_dir / self._STORAGE_SUBDIR / self._STORAGE_SUBSUBDIR_DEFAULT

        return self._storage_dir / self._STORAGE_SUBDIR / self._metadata.name

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the request queue metadata file."""
        return self.path_to_rq / METADATA_FILENAME

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> FileSystemRequestQueueClient:
        """Open or create a file system request queue client.

        This method attempts to open an existing request queue from the file system. If a queue with the specified
        ID or name exists, it loads the metadata and state from the stored files. If no existing queue is found,
        a new one is created.

        Args:
            id: The ID of the request queue to open. If provided, searches for existing queue by ID.
            name: The name of the request queue to open. If not provided, uses the default queue.
            configuration: The configuration object containing storage directory settings.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a queue with the specified ID is not found, or if metadata is invalid.
        """
        storage_dir = Path(configuration.storage_dir)
        rq_base_path = storage_dir / cls._STORAGE_SUBDIR

        if not rq_base_path.exists():
            await asyncio.to_thread(rq_base_path.mkdir, parents=True, exist_ok=True)

        # Open an existing RQ by its ID, raise an error if not found.
        if id:
            found = False
            for rq_dir in rq_base_path.iterdir():
                if not rq_dir.is_dir():
                    continue

                metadata_path = rq_dir / METADATA_FILENAME
                if not metadata_path.exists():
                    continue

                try:
                    file = await asyncio.to_thread(metadata_path.open)
                    try:
                        file_content = json.load(file)
                        metadata = RequestQueueMetadata(**file_content)

                        if metadata.id == id:
                            client = cls(
                                metadata=metadata,
                                storage_dir=storage_dir,
                                lock=asyncio.Lock(),
                            )
                            await client._state.initialize()
                            await client._discover_existing_requests()
                            await client._update_metadata(update_accessed_at=True)
                            found = True
                            break
                    finally:
                        await asyncio.to_thread(file.close)
                except (json.JSONDecodeError, ValidationError):
                    continue

            if not found:
                raise ValueError(f'Request queue with ID "{id}" not found')

        # Open an existing RQ by its name, or create a new one if not found.
        else:
            rq_path = rq_base_path / cls._STORAGE_SUBSUBDIR_DEFAULT if name is None else rq_base_path / name
            metadata_path = rq_path / METADATA_FILENAME

            # If the RQ directory exists, reconstruct the client from the metadata file.
            if rq_path.exists() and metadata_path.exists():
                file = await asyncio.to_thread(open, metadata_path)
                try:
                    file_content = json.load(file)
                finally:
                    await asyncio.to_thread(file.close)
                try:
                    metadata = RequestQueueMetadata(**file_content)
                except ValidationError as exc:
                    raise ValueError(f'Invalid metadata file for request queue "{name}"') from exc

                metadata.name = name

                client = cls(
                    metadata=metadata,
                    storage_dir=storage_dir,
                    lock=asyncio.Lock(),
                )

                await client._state.initialize()
                await client._discover_existing_requests()
                await client._update_metadata(update_accessed_at=True)

            # Otherwise, create a new dataset client.
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
                client = cls(
                    metadata=metadata,
                    storage_dir=storage_dir,
                    lock=asyncio.Lock(),
                )
                await client._state.initialize()
                await client._update_metadata()

        return client

    @override
    async def drop(self) -> None:
        async with self._lock:
            # Remove the RQ dir recursively if it exists.
            if self.path_to_rq.exists():
                await asyncio.to_thread(shutil.rmtree, self.path_to_rq)

            # Clear recoverable state
            await self._state.reset()
            await self._state.teardown()
            self._request_cache.clear()
            self._request_cache_needs_refresh = True

            # Invalidate is_empty cache.
            self._is_empty_cache = None

    @override
    async def purge(self) -> None:
        async with self._lock:
            request_files = await self._get_request_files(self.path_to_rq)

            for file_path in request_files:
                await asyncio.to_thread(file_path.unlink, missing_ok=True)

            # Clear recoverable state
            await self._state.reset()
            self._request_cache.clear()
            self._request_cache_needs_refresh = True

            await self._update_metadata(
                update_modified_at=True,
                update_accessed_at=True,
                new_pending_request_count=0,
            )

            # Invalidate is_empty cache.
            self._is_empty_cache = None

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        async with self._lock:
            self._is_empty_cache = None
            new_total_request_count = self._metadata.total_request_count
            new_pending_request_count = self._metadata.pending_request_count
            processed_requests = list[ProcessedRequest]()
            unprocessed_requests = list[UnprocessedRequest]()
            state = self._state.current_value

            # Prepare a dictionary to track existing requests by their unique keys.
            existing_unique_keys: dict[str, Path] = {}
            existing_request_files = await self._get_request_files(self.path_to_rq)

            for request_file in existing_request_files:
                existing_request = await self._parse_request_file(request_file)
                if existing_request is not None:
                    existing_unique_keys[existing_request.unique_key] = request_file

            # Process each request in the batch.
            for request in requests:
                existing_request_file = existing_unique_keys.get(request.unique_key)
                existing_request = None

                # Only load the full request from disk if we found a duplicate
                if existing_request_file is not None:
                    existing_request = await self._parse_request_file(existing_request_file)

                # If there is no existing request with the same unique key, add the new request.
                if existing_request is None:
                    request_path = self._get_request_path(request.id)

                    # Add sequence number to ensure FIFO ordering using state.
                    if forefront:
                        sequence_number = state.forefront_sequence_counter
                        state.forefront_sequence_counter += 1
                        state.forefront_requests[request.id] = sequence_number
                    else:
                        sequence_number = state.sequence_counter
                        state.sequence_counter += 1
                        state.regular_requests[request.id] = sequence_number

                    # Save the clean request without extra fields
                    request_data = await json_dumps(request.model_dump())
                    await atomic_write(request_path, request_data)

                    # Update the metadata counts.
                    new_total_request_count += 1
                    new_pending_request_count += 1

                    # Add to our index for subsequent requests in this batch
                    existing_unique_keys[request.unique_key] = self._get_request_path(request.id)

                    processed_requests.append(
                        ProcessedRequest(
                            id=request.id,
                            unique_key=request.unique_key,
                            was_already_present=False,
                            was_already_handled=False,
                        )
                    )

                # If the request already exists in the RQ, just update it if needed.
                else:
                    # Set the processed request flags.
                    was_already_present = existing_request is not None
                    was_already_handled = existing_request.id in state.handled_requests

                    # If the request is already in the RQ and handled, just continue with the next one.
                    if was_already_present and was_already_handled:
                        processed_requests.append(
                            ProcessedRequest(
                                id=existing_request.id,
                                unique_key=request.unique_key,
                                was_already_present=True,
                                was_already_handled=True,
                            )
                        )

                    # If the request is already in the RQ but not handled yet, update it.
                    elif was_already_present and not was_already_handled:
                        # Update request type (forefront vs regular) in state
                        if forefront:
                            # Move from regular to forefront if needed
                            if existing_request.id in state.regular_requests:
                                state.regular_requests.pop(existing_request.id)
                            if existing_request.id not in state.forefront_requests:
                                state.forefront_requests[existing_request.id] = state.forefront_sequence_counter
                                state.forefront_sequence_counter += 1
                        elif (
                            existing_request.id not in state.forefront_requests
                            and existing_request.id not in state.regular_requests
                        ):
                            # Keep as regular if not already forefront
                            state.regular_requests[existing_request.id] = state.sequence_counter
                            state.sequence_counter += 1

                        processed_requests.append(
                            ProcessedRequest(
                                id=existing_request.id,
                                unique_key=request.unique_key,
                                was_already_present=True,
                                was_already_handled=False,
                            )
                        )

                    else:
                        logger.warning(f'Request with unique key "{request.unique_key}" could not be processed.')
                        unprocessed_requests.append(
                            UnprocessedRequest(
                                unique_key=request.unique_key,
                                url=request.url,
                                method=request.method,
                            )
                        )

            await self._update_metadata(
                update_modified_at=True,
                update_accessed_at=True,
                new_total_request_count=new_total_request_count,
                new_pending_request_count=new_pending_request_count,
            )

            # Invalidate the cache if we added forefront requests.
            if forefront:
                self._request_cache_needs_refresh = True

            # Invalidate is_empty cache.
            self._is_empty_cache = None

            return AddRequestsResponse(
                processed_requests=processed_requests,
                unprocessed_requests=unprocessed_requests,
            )

    @override
    async def get_request(self, request_id: str) -> Request | None:
        async with self._lock:
            request_path = self._get_request_path(request_id)
            request = await self._parse_request_file(request_path)

            if request is None:
                logger.warning(f'Request with ID "{request_id}" not found in the queue.')
                return None

            state = self._state.current_value
            state.in_progress_requests.add(request.id)
            await self._update_metadata(update_accessed_at=True)
            return request

    @override
    async def fetch_next_request(self) -> Request | None:
        async with self._lock:
            # Refresh cache if needed or if it's empty.
            if self._request_cache_needs_refresh or not self._request_cache:
                await self._refresh_cache()

            next_request: Request | None = None
            state = self._state.current_value

            # Fetch from the front of the deque (forefront requests are at the beginning).
            while self._request_cache and next_request is None:
                candidate = self._request_cache.popleft()

                # Skip requests that are already in progress, however this should not happen.
                if candidate.id not in state.in_progress_requests:
                    next_request = candidate

            if next_request is not None:
                state.in_progress_requests.add(next_request.id)

            return next_request

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        async with self._lock:
            self._is_empty_cache = None
            state = self._state.current_value

            # Check if the request is in progress.
            if request.id not in state.in_progress_requests:
                logger.warning(f'Marking request {request.id} as handled that is not in progress.')
                return None

            # Update the request's handled_at timestamp.
            if request.handled_at is None:
                request.handled_at = datetime.now(timezone.utc)

            # Dump the updated request to the file.
            request_path = self._get_request_path(request.id)

            if not await asyncio.to_thread(request_path.exists):
                logger.warning(f'Request file for {request.id} does not exist, cannot mark as handled.')
                return None

            request_data = await json_dumps(request.model_dump())
            await atomic_write(request_path, request_data)

            # Update state: remove from in-progress and add to handled.
            state.in_progress_requests.discard(request.id)
            state.handled_requests.add(request.id)

            # Update RQ metadata.
            await self._update_metadata(
                update_modified_at=True,
                update_accessed_at=True,
                new_handled_request_count=self._metadata.handled_request_count + 1,
                new_pending_request_count=self._metadata.pending_request_count - 1,
            )

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
        async with self._lock:
            self._is_empty_cache = None
            state = self._state.current_value

            # Check if the request is in progress.
            if request.id not in state.in_progress_requests:
                logger.info(f'Reclaiming request {request.id} that is not in progress.')
                return None

            request_path = self._get_request_path(request.id)

            if not await asyncio.to_thread(request_path.exists):
                logger.warning(f'Request file for {request.id} does not exist, cannot reclaim.')
                return None

            # Update sequence number and state to ensure proper ordering.
            if forefront:
                # Remove from regular requests if it was there
                state.regular_requests.pop(request.id, None)
                sequence_number = state.forefront_sequence_counter
                state.forefront_sequence_counter += 1
                state.forefront_requests[request.id] = sequence_number
            else:
                # Remove from forefront requests if it was there
                state.forefront_requests.pop(request.id, None)
                sequence_number = state.sequence_counter
                state.sequence_counter += 1
                state.regular_requests[request.id] = sequence_number

            # Save the clean request without extra fields
            request_data = await json_dumps(request.model_dump())
            await atomic_write(request_path, request_data)

            # Remove from in-progress.
            state.in_progress_requests.discard(request.id)

            # Update RQ metadata.
            await self._update_metadata(
                update_modified_at=True,
                update_accessed_at=True,
            )

            # Add the request back to the cache.
            if forefront:
                self._request_cache.appendleft(request)
            else:
                self._request_cache.append(request)

            return ProcessedRequest(
                id=request.id,
                unique_key=request.unique_key,
                was_already_present=True,
                was_already_handled=False,
            )

    @override
    async def is_empty(self) -> bool:
        async with self._lock:
            # If we have a cached value, return it immediately.
            if self._is_empty_cache is not None:
                return self._is_empty_cache

            state = self._state.current_value

            # If there are in-progress requests, return False immediately.
            if len(state.in_progress_requests) > 0:
                self._is_empty_cache = False
                return False

            # If we have a cached requests, check them first (fast path).
            if self._request_cache:
                for req in self._request_cache:
                    if req.id not in state.handled_requests:
                        self._is_empty_cache = False
                        return False
                self._is_empty_cache = True
                return len(state.in_progress_requests) == 0

            # Fallback: check state for unhandled requests.
            await self._update_metadata(update_accessed_at=True)

            # Check if there are any requests that are not handled
            all_requests = set(state.forefront_requests.keys()) | set(state.regular_requests.keys())
            unhandled_requests = all_requests - state.handled_requests

            if unhandled_requests:
                self._is_empty_cache = False
                return False

            self._is_empty_cache = True
            return True

    def _get_request_path(self, request_id: str) -> Path:
        """Get the path to a specific request file.

        Args:
            request_id: The ID of the request.

        Returns:
            The path to the request file.
        """
        return self.path_to_rq / f'{request_id}.json'

    async def _update_metadata(
        self,
        *,
        new_handled_request_count: int | None = None,
        new_pending_request_count: int | None = None,
        new_total_request_count: int | None = None,
        update_had_multiple_clients: bool = False,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the dataset metadata file with current information.

        Args:
            new_handled_request_count: If provided, update the handled_request_count to this value.
            new_pending_request_count: If provided, update the pending_request_count to this value.
            new_total_request_count: If provided, update the total_request_count to this value.
            update_had_multiple_clients: If True, set had_multiple_clients to True.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        # Always create a new timestamp to ensure it's truly updated
        now = datetime.now(timezone.utc)

        # Update timestamps according to parameters
        if update_accessed_at:
            self._metadata.accessed_at = now

        if update_modified_at:
            self._metadata.modified_at = now

        # Update request counts if provided
        if new_handled_request_count is not None:
            self._metadata.handled_request_count = new_handled_request_count

        if new_pending_request_count is not None:
            self._metadata.pending_request_count = new_pending_request_count

        if new_total_request_count is not None:
            self._metadata.total_request_count = new_total_request_count

        if update_had_multiple_clients:
            self._metadata.had_multiple_clients = True

        # Ensure the parent directory for the metadata file exists.
        await asyncio.to_thread(self.path_to_metadata.parent.mkdir, parents=True, exist_ok=True)

        # Dump the serialized metadata to the file.
        data = await json_dumps(self._metadata.model_dump())
        await atomic_write(self.path_to_metadata, data)

    async def _refresh_cache(self) -> None:
        """Refresh the request cache from filesystem.

        This method loads up to _MAX_REQUESTS_IN_CACHE requests from the filesystem,
        prioritizing forefront requests and maintaining proper ordering.
        """
        self._request_cache.clear()
        state = self._state.current_value

        forefront_requests = list[tuple[Request, int]]()  # (request, sequence)
        regular_requests = list[tuple[Request, int]]()  # (request, sequence)

        request_files = await self._get_request_files(self.path_to_rq)

        for request_file in request_files:
            request = await self._parse_request_file(request_file)

            if request is None:
                continue

            # Skip handled requests
            if request.id in state.handled_requests:
                continue

            # Skip in-progress requests
            if request.id in state.in_progress_requests:
                continue

            # Determine if request is forefront or regular based on state
            if request.id in state.forefront_requests:
                sequence = state.forefront_requests[request.id]
                forefront_requests.append((request, sequence))
            elif request.id in state.regular_requests:
                sequence = state.regular_requests[request.id]
                regular_requests.append((request, sequence))
            else:
                # Request not in state, skip it (might be orphaned)
                logger.warning(f'Request {request.id} not found in state, skipping.')
                continue

        # Sort forefront requests by sequence (newest first for LIFO behavior).
        forefront_requests.sort(key=lambda item: item[1], reverse=True)

        # Sort regular requests by sequence (oldest first for FIFO behavior).
        regular_requests.sort(key=lambda item: item[1], reverse=False)

        # Add forefront requests to the beginning of the cache (left side). Since forefront_requests are sorted
        # by sequence (newest first), we need to add them in reverse order to maintain correct priority.
        for request, _ in reversed(forefront_requests):
            if len(self._request_cache) >= self._MAX_REQUESTS_IN_CACHE:
                break
            self._request_cache.appendleft(request)

        # Add regular requests to the end of the cache (right side).
        for request, _ in regular_requests:
            if len(self._request_cache) >= self._MAX_REQUESTS_IN_CACHE:
                break
            self._request_cache.append(request)

        self._request_cache_needs_refresh = False

    @classmethod
    async def _get_request_files(cls, path_to_rq: Path) -> list[Path]:
        """Get all request files from the RQ.

        Args:
            path_to_rq: The path to the request queue directory.

        Returns:
            A list of paths to all request files.
        """
        # Create the requests directory if it doesn't exist.
        await asyncio.to_thread(path_to_rq.mkdir, parents=True, exist_ok=True)

        # List all the json files.
        files = await asyncio.to_thread(list, path_to_rq.glob('*.json'))

        # Filter out metadata file and non-file entries.
        filtered = filter(
            lambda request_file: request_file.is_file() and request_file.name != METADATA_FILENAME,
            files,
        )

        return list(filtered)

    @classmethod
    async def _parse_request_file(cls, file_path: Path) -> Request | None:
        """Parse a request file and return the `Request` object.

        Args:
            file_path: The path to the request file.

        Returns:
            The parsed `Request` object or `None` if the file could not be read or parsed.
        """
        # Open the request file.
        try:
            file = await asyncio.to_thread(open, file_path)
        except FileNotFoundError:
            logger.warning(f'Request file "{file_path}" not found.')
            return None

        # Read the file content and parse it as JSON.
        try:
            file_content = json.load(file)
        except json.JSONDecodeError as exc:
            logger.warning(f'Failed to parse request file {file_path}: {exc!s}')
            return None
        finally:
            await asyncio.to_thread(file.close)

        # Validate the content against the Request model.
        try:
            return Request.model_validate(file_content)
        except ValidationError as exc:
            logger.warning(f'Failed to validate request file {file_path}: {exc!s}')
            return None

    async def _discover_existing_requests(self) -> None:
        """Discover and load existing requests into the state when opening an existing request queue."""
        request_files = await self._get_request_files(self.path_to_rq)
        state = self._state.current_value

        for request_file in request_files:
            request = await self._parse_request_file(request_file)
            if request is None:
                continue

            # Add request to state as regular request (assign sequence numbers)
            if request.id not in state.regular_requests and request.id not in state.forefront_requests:
                # Assign as regular request with current sequence counter
                state.regular_requests[request.id] = state.sequence_counter
                state.sequence_counter += 1

                # Check if request was already handled
                if request.handled_at is not None:
                    state.handled_requests.add(request.id)
