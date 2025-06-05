from __future__ import annotations

import asyncio
import json
import shutil
from collections import deque
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ValidationError
from typing_extensions import override

from crawlee import Request
from crawlee._consts import METADATA_FILENAME
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import atomic_write, json_dumps
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


class FileSystemRequestQueueClient(RequestQueueClient):
    """A file system implementation of the request queue client.

    This client persists requests to the file system as individual JSON files, making it suitable for scenarios
    where data needs to survive process restarts. Each request is stored as a separate file in a directory
    structure following the pattern:

    ```
    {STORAGE_DIR}/request_queues/{QUEUE_ID}/{REQUEST_ID}.json
    ```

    The implementation uses sequence numbers embedded in request files for FIFO ordering of regular requests.
    It maintains in-memory data structures for tracking in-progress requests and prioritizing forefront requests.
    File system storage provides durability at the cost of slower I/O operations compared to memory-based storage.

    This implementation is ideal for long-running crawlers where persistence is important and for situations
    where you need to resume crawling after process termination.
    """

    _STORAGE_SUBDIR = 'request_queues'
    """The name of the subdirectory where request queues are stored."""

    _STORAGE_SUBSUBDIR_DEFAULT = 'default'
    """The name of the subdirectory for the default request queue."""

    def __init__(
        self,
        *,
        id: str,
        name: str | None,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        had_multiple_clients: bool,
        handled_request_count: int,
        pending_request_count: int,
        stats: dict,
        total_request_count: int,
        storage_dir: Path,
        sequence_counter: int,
        forefront_sequence_counter: int,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemRequestQueueClient.open` class method to create a new instance.
        """
        self._metadata = RequestQueueMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
            had_multiple_clients=had_multiple_clients,
            handled_request_count=handled_request_count,
            pending_request_count=pending_request_count,
            stats=stats,
            total_request_count=total_request_count,
        )

        self._storage_dir = storage_dir
        """The base directory where the request queue is stored."""

        self._sequence_counter = sequence_counter
        """A counter to track the order of (normal) requests added to the queue.

        This number is going to be used as a sequence number for next request.
        """

        self._forefront_sequence_counter = forefront_sequence_counter
        """A counter to track the order of forefront requests added to the queue.

        This number is going to be used as a sequence number for next forefront request.
        """

        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

        self._in_progress = set[str]()
        """A set of request IDs that are currently being processed."""

        self._cache_size = 50
        """Maximum number of requests to keep in cache."""

        self._request_cache = deque[Request]()
        """Cache for requests: forefront requests at the beginning, regular requests at the end."""

        self._cache_needs_refresh = True
        """Flag indicating whether the cache needs to be refreshed from filesystem."""

    @property
    @override
    def metadata(self) -> RequestQueueMetadata:
        return self._metadata

    @property
    def path_to_rq(self) -> Path:
        """The full path to the request queue directory."""
        if self.metadata.name is None:
            return self._storage_dir / self._STORAGE_SUBDIR / self._STORAGE_SUBSUBDIR_DEFAULT

        return self._storage_dir / self._STORAGE_SUBDIR / self.metadata.name

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the request queue metadata file."""
        return self.path_to_rq / METADATA_FILENAME

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> FileSystemRequestQueueClient:
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

                        rq_path = (
                            rq_base_path / cls._STORAGE_SUBSUBDIR_DEFAULT
                            if metadata.name is None
                            else rq_base_path / metadata.name
                        )
                        sequence_counter, forefront_sequence_counter = await cls._get_sequence_counters(rq_path)

                        if metadata.id == id:
                            client = cls(
                                **metadata.model_dump(),
                                storage_dir=storage_dir,
                                sequence_counter=sequence_counter,
                                forefront_sequence_counter=forefront_sequence_counter,
                            )
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
            if rq_path.exists():
                # If metadata file is missing, raise an error.
                if not metadata_path.exists():
                    raise ValueError(f'Metadata file not found for request queue "{name}"')

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
                sequence_counter, forefront_sequence_counter = await cls._get_sequence_counters(rq_path)

                client = cls(
                    **metadata.model_dump(),
                    storage_dir=storage_dir,
                    sequence_counter=sequence_counter,
                    forefront_sequence_counter=forefront_sequence_counter,
                )

                await client._update_metadata(update_accessed_at=True)

            # Otherwise, create a new dataset client.
            else:
                now = datetime.now(timezone.utc)
                client = cls(
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
                    storage_dir=storage_dir,
                    sequence_counter=0,
                    forefront_sequence_counter=0,
                )
                await client._update_metadata()

        return client

    @override
    async def drop(self) -> None:
        async with self._lock:
            # Remove the RQ dir recursively if it exists.
            if self.path_to_rq.exists():
                await asyncio.to_thread(shutil.rmtree, self.path_to_rq)

            self._in_progress.clear()
            self._request_cache.clear()
            self._cache_needs_refresh = True

    @override
    async def purge(self) -> None:
        async with self._lock:
            request_files = await self._get_request_files(self.path_to_rq)

            for file_path in request_files:
                await asyncio.to_thread(file_path.unlink)

            self._in_progress.clear()
            self._request_cache.clear()
            self._cache_needs_refresh = True

            # Update metadata counts
            await self._update_metadata(
                update_modified_at=True,
                update_accessed_at=True,
                new_pending_request_count=0,
            )

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        """Add a batch of requests to the queue.

        Args:
            requests: The requests to add.
            forefront: Whether to add the requests to the beginning of the queue.

        Returns:
            Response containing information about the added requests.
        """
        async with self._lock:
            new_total_request_count = self._metadata.total_request_count
            new_pending_request_count = self._metadata.pending_request_count
            processed_requests = list[ProcessedRequest]()
            unprocessed_requests = list[UnprocessedRequest]()

            for request in requests:
                existing_request_files = await self._get_request_files(self.path_to_rq)
                existing_request = None

                # Go through existing requests to find if the request already exists in the queue.
                for existing_request_file in existing_request_files:
                    existing_request = await self._parse_request_file(existing_request_file)

                    if existing_request is None:
                        continue

                    # If the unique key matches, we found an existing request
                    if existing_request.unique_key == request.unique_key:
                        break

                    existing_request = None

                # If there is no existing request with the same unique key, add the new request.
                if existing_request is None:
                    request_path = self._get_request_path(request.id)

                    # Add sequence number to ensure FIFO ordering.
                    if forefront:
                        sequence_number = self._forefront_sequence_counter
                        self._forefront_sequence_counter += 1
                    else:
                        sequence_number = self._sequence_counter
                        self._sequence_counter += 1

                    # Update the request data and dump it to the file.
                    request_dict = request.model_dump()
                    request_dict['__sequence'] = sequence_number
                    request_dict['__forefront'] = forefront
                    request_data = await json_dumps(request_dict)
                    await atomic_write(request_path, request_data)

                    # Update the metadata counts.
                    new_total_request_count += 1
                    new_pending_request_count += 1

                    processed_requests.append(
                        ProcessedRequest(
                            id=request.id,
                            unique_key=request.unique_key,
                            was_already_present=False,
                            was_already_handled=False,
                        )
                    )

                # If the request already exists, we need to update it.
                else:
                    # Set the processed request flags.
                    was_already_present = existing_request is not None
                    was_already_handled = existing_request.was_already_handled if existing_request else False

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
                        request_path = self._get_request_path(request.id)
                        request_dict = existing_request.model_dump()
                        request_dict['__forefront'] = forefront
                        request_data = await json_dumps(existing_request.model_dump())
                        await atomic_write(request_path, request_data)

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
                self._cache_needs_refresh = True

            return AddRequestsResponse(
                processed_requests=processed_requests,
                unprocessed_requests=unprocessed_requests,
            )

    @override
    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve.

        Returns:
            The retrieved request, or None, if it did not exist.
        """
        request_path = self._get_request_path(request_id)
        request = await self._parse_request_file(request_path)

        if request is None:
            logger.warning(f'Request with ID "{request_id}" not found in the queue.')
            return None

        self._in_progress.add(request.id)
        return request

    @override
    async def fetch_next_request(self) -> Request | None:
        """Return the next request in the queue to be processed.

        Once you successfully finish processing of the request, you need to call `RequestQueue.mark_request_as_handled`
        to mark the request as handled in the queue. If there was some error in processing the request, call
        `RequestQueue.reclaim_request` instead, so that the queue will give the request to some other consumer
        in another call to the `fetch_next_request` method.

        Returns:
            The request or `None` if there are no more pending requests.
        """
        async with self._lock:
            # Refresh cache if needed or if it's empty.
            if self._cache_needs_refresh or not self._request_cache:
                await self._refresh_cache()

            next_request: Request | None = None

            # Fetch from the front of the deque (forefront requests are at the beginning).
            while self._request_cache and next_request is None:
                candidate = self._request_cache.popleft()

                # Skip requests that are already in progress, however this should not happen.
                if candidate.id not in self._in_progress:
                    next_request = candidate

            # If cache is getting low, mark for refresh on next call.
            if len(self._request_cache) < self._cache_size // 4:
                self._cache_needs_refresh = True

            if next_request is not None:
                self._in_progress.add(next_request.id)

            return next_request

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        async with self._lock:
            # Check if the request is in progress.
            if request.id not in self._in_progress:
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

            # Remove from in-progress.
            self._in_progress.discard(request.id)

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
        """Reclaim a failed request back to the queue.

        The request will be returned for processing later again by another call to `fetch_next_request`.

        Args:
            request: The request to return to the queue.
            forefront: Whether to add the request to the head or the end of the queue.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        async with self._lock:
            # Check if the request is in progress.
            if request.id not in self._in_progress:
                logger.info(f'Reclaiming request {request.id} that is not in progress.')
                return None

            request_path = self._get_request_path(request.id)

            if not await asyncio.to_thread(request_path.exists):
                logger.warning(f'Request file for {request.id} does not exist, cannot reclaim.')
                return None

            # Update sequence number to ensure proper ordering.
            if forefront:
                sequence_number = self._forefront_sequence_counter
                self._forefront_sequence_counter += 1
            else:
                sequence_number = self._sequence_counter
                self._sequence_counter += 1

            # Dump the updated request to the file.
            request_dict = request.model_dump()
            request_dict['__forefront'] = forefront
            request_dict['__sequence'] = sequence_number
            request_data = await json_dumps(request_dict)
            await atomic_write(request_path, request_data)

            # Remove from in-progress.
            self._in_progress.discard(request.id)

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
        """Check if the queue is empty.

        Returns:
            True if the queue is empty, False otherwise.
        """
        async with self._lock:
            await self._update_metadata(update_accessed_at=True)
            request_files = await self._get_request_files(self.path_to_rq)

            # Check each file to see if there are any unhandled requests.
            for request_file in request_files:
                request = await self._parse_request_file(request_file)

                if request is None:
                    continue

                # If any request is not handled, the queue is not empty.
                if request.handled_at is None:
                    return False

        # If we got here, all requests are handled or there are no requests.
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

        This method loads up to _cache_size requests from the filesystem,
        prioritizing forefront requests and maintaining proper ordering.
        """
        self._request_cache.clear()

        request_files = await self._get_request_files(self.path_to_rq)

        forefront_requests = []
        regular_requests = []

        for request_file in request_files:
            request = await self._parse_request_file(request_file)

            if request is None or request.was_already_handled:
                continue

            if request.id in self._in_progress:
                continue

            if request.model_extra is None:
                logger.warning(f'Request file "{request_file}" does not contain model_extra field.')
                continue

            forefront = request.model_extra.get('__forefront')
            if forefront is None:
                logger.warning(f'Request file "{request_file}" does not contain "__forefront" field.')
                continue

            if forefront:
                forefront_requests.append(request)
            else:
                regular_requests.append(request)

        # Sort forefront requests by sequence (newest first for LIFO behavior).
        forefront_requests.sort(
            key=lambda request: request.model_extra.get('__sequence', 0) if request.model_extra else 0,
            reverse=True,
        )

        # Sort regular requests by sequence (oldest first for FIFO behavior).
        regular_requests.sort(
            key=lambda request: request.model_extra.get('__sequence', 0) if request.model_extra else 0,
            reverse=False,
        )

        # Add forefront requests to the beginning of the cache (left side). Since forefront_requests are sorted
        # by sequence (newest first), we need to add them in reverse order to maintain correct priority.
        for request in reversed(forefront_requests):
            if len(self._request_cache) >= self._cache_size:
                break
            self._request_cache.appendleft(request)

        # Add regular requests to the end of the cache (right side).
        for request in regular_requests:
            if len(self._request_cache) >= self._cache_size:
                break
            self._request_cache.append(request)

        self._cache_needs_refresh = False

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

    @classmethod
    async def _get_sequence_counters(cls, path_to_rq: Path) -> tuple[int, int]:
        """Get the current sequence counters for the request queue.

        Args:
            path_to_rq: The path to the request queue directory.

        Returns:
            A tuple containing the current sequence counter for regular requests and for forefront requests.
        """
        max_sequence = -1
        max_forefront_sequence = -1

        # Get all request files.
        request_files = await cls._get_request_files(path_to_rq)

        for request_file in request_files:
            request = await cls._parse_request_file(request_file)
            if request is None:
                continue

            # Extract sequence number and forefront flag from model_extra.
            if request.model_extra:
                sequence = request.model_extra.get('__sequence')
                is_forefront = request.model_extra.get('__forefront')

                if sequence is None:
                    logger.warning(f'Request file "{request_file}" does not contain "__sequence" field.')
                    continue

                if is_forefront is None:
                    logger.warning(f'Request file "{request_file}" does not contain "__forefront" field.')
                    continue

                if is_forefront:
                    max_forefront_sequence = max(max_forefront_sequence, sequence)
                else:
                    max_sequence = max(max_sequence, sequence)

        return max_sequence, max_forefront_sequence
