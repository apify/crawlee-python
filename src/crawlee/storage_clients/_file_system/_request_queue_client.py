from __future__ import annotations

import asyncio
import json
import shutil
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
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

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

        # Internal attributes
        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

        self._in_progress = set[str]()
        """A set of request IDs that are currently being processed."""

        self._forefront_requests = list[str]()
        """A list of request IDs that should be prioritized (added with forefront=True).
        Most recent forefront requests are added at the beginning of the list."""

        self._sequence_counter = 0
        """A counter to track the order of requests added to the queue."""

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

        # Get a new instance by ID.
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
                                id=metadata.id,
                                name=metadata.name,
                                created_at=metadata.created_at,
                                accessed_at=metadata.accessed_at,
                                modified_at=metadata.modified_at,
                                had_multiple_clients=metadata.had_multiple_clients,
                                handled_request_count=metadata.handled_request_count,
                                pending_request_count=metadata.pending_request_count,
                                stats=metadata.stats,
                                total_request_count=metadata.total_request_count,
                                storage_dir=storage_dir,
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

        # Get a new instance by name.
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

                client = cls(
                    id=metadata.id,
                    name=name,
                    created_at=metadata.created_at,
                    accessed_at=metadata.accessed_at,
                    modified_at=metadata.modified_at,
                    had_multiple_clients=metadata.had_multiple_clients,
                    handled_request_count=metadata.handled_request_count,
                    pending_request_count=metadata.pending_request_count,
                    stats=metadata.stats,
                    total_request_count=metadata.total_request_count,
                    storage_dir=storage_dir,
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
                )
                await client._update_metadata()

        return client

    @override
    async def drop(self) -> None:
        # If the client directory exists, remove it recursively.
        if self.path_to_rq.exists():
            async with self._lock:
                await asyncio.to_thread(shutil.rmtree, self.path_to_rq)

    @override
    async def purge(self) -> None:
        async with self._lock:
            for file_path in self.path_to_rq.glob('*'):
                if file_path.name == METADATA_FILENAME:
                    continue
                await asyncio.to_thread(file_path.unlink)

            # Update metadata counts
            await self._update_metadata(
                update_modified_at=True,
                update_accessed_at=True,
                new_handled_request_count=0,
                new_pending_request_count=0,
                new_total_request_count=0,
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

            for request in requests:
                # Go through existing requests to find if the request already exists in the queue.
                existing_request_files = await self._get_request_files()
                existing_request = None

                for existing_request_file in existing_request_files:
                    existing_request = await self._parse_request_file(existing_request_file)

                    if existing_request is None:
                        continue

                    # If the unique key matches, we found an existing request
                    if existing_request.unique_key == request.unique_key:
                        break

                    existing_request = None

                # Set the processed request flags.
                was_already_present = existing_request is not None
                was_already_handled = (
                    was_already_present and existing_request and existing_request.handled_at is not None
                )

                # If the request is already in the queue and handled, do not enqueue it again.
                if was_already_handled and existing_request:
                    processed_requests.append(
                        ProcessedRequest(
                            id=existing_request.id,
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=True,
                        )
                    )
                    continue

                # If forefront and existing request is not handled, mark it as forefront.
                if forefront and was_already_present and not was_already_handled and existing_request:
                    self._forefront_requests.insert(0, existing_request.id)
                    processed_requests.append(
                        ProcessedRequest(
                            id=existing_request.id,
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=False,
                        )
                    )
                    continue

                # If the request is already in the queue but not handled, update it.
                if was_already_present and existing_request:
                    # Update the existing request file
                    request_path = self.path_to_rq / f'{existing_request.id}.json'
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
                    continue

                # Add the new request to the queue
                request_path = self.path_to_rq / f'{request.id}.json'

                # Create a data dictionary from the request and remove handled_at if it's None
                request_dict = request.model_dump()
                if request_dict.get('handled_at') is None:
                    request_dict.pop('handled_at', None)

                # Add sequence number to ensure FIFO ordering
                sequence_number = self._sequence_counter
                self._sequence_counter += 1
                request_dict['sequence'] = sequence_number

                request_data = await json_dumps(request_dict)
                await atomic_write(request_path, request_data)

                # Update metadata counts
                new_total_request_count += 1
                new_pending_request_count += 1

                # If forefront, add to the forefront list
                if forefront:
                    self._forefront_requests.insert(0, request.id)

                processed_requests.append(
                    ProcessedRequest(
                        id=request.id,
                        unique_key=request.unique_key,
                        was_already_present=False,
                        was_already_handled=False,
                    )
                )

            await self._update_metadata(
                update_modified_at=True,
                update_accessed_at=True,
                new_total_request_count=new_total_request_count,
                new_pending_request_count=new_pending_request_count,
            )

            return AddRequestsResponse(
                processed_requests=processed_requests,
                unprocessed_requests=[],
            )

    @override
    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve.

        Returns:
            The retrieved request, or None, if it did not exist.
        """
        request_path = self.path_to_rq / f'{request_id}.json'
        request = await self._parse_request_file(request_path)
        if request is None:
            logger.warning(f'Request with ID "{request_id}" not found in the queue.')
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
            # Create the requests directory if it doesn't exist
            await asyncio.to_thread(self.path_to_rq.mkdir, parents=True, exist_ok=True)

            # First try forefront requests (highest priority)
            forefront_request = await self._try_get_forefront_request()
            if forefront_request is not None:
                return forefront_request

            # Collect and categorize regular requests
            request_sequences, requests_without_sequence = await self._categorize_regular_requests()

            # Try to get a request with a sequence number (FIFO order)
            sequenced_request = await self._try_get_sequenced_request(request_sequences)
            if sequenced_request is not None:
                return sequenced_request

            # Fall back to requests without sequence numbers (using file timestamps)
            return await self._try_get_non_sequenced_request(requests_without_sequence)

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
            # Check if the request is in progress
            if request.id not in self._in_progress:
                return None

            # Remove from in-progress set
            self._in_progress.discard(request.id)

            # Update the request object - set handled_at timestamp
            if request.handled_at is None:
                request.handled_at = datetime.now(timezone.utc)

            # Write the updated request back to the requests directory
            request_path = self.path_to_rq / f'{request.id}.json'

            if not await asyncio.to_thread(request_path.exists):
                return None

            request_data = await json_dumps(request.model_dump())
            await atomic_write(request_path, request_data)

            # Update RQ metadata
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
            # Check if the request is in progress
            if request.id not in self._in_progress:
                return None

            # Remove from in-progress set
            self._in_progress.discard(request.id)

            # If forefront is true, mark this request as priority
            if forefront:
                self._forefront_requests.insert(0, request.id)
            # Make sure it's not in the forefront list if it was previously added there
            elif request.id in self._forefront_requests:
                self._forefront_requests.remove(request.id)

            # To simulate changing the file timestamp for FIFO ordering,
            # we'll update the file with current timestamp
            request_path = self.path_to_rq / f'{request.id}.json'

            if not await asyncio.to_thread(request_path.exists):
                return None

            request_data = await json_dumps(request.model_dump())
            await atomic_write(request_path, request_data)

            # Update metadata timestamps
            await self._update_metadata(update_modified_at=True, update_accessed_at=True)

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
            # Update accessed timestamp when checking if queue is empty
            await self._update_metadata(update_accessed_at=True)

            request_files = await self._get_request_files()

            # Check each file to see if there are any unhandled requests
            for request_file in request_files:
                request = await self._parse_request_file(request_file)

                if request is None:
                    continue

                # If any request is not handled, the queue is not empty
                if request.handled_at is None:
                    return False

        # If we got here, all requests are handled or there are no requests
        return True

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

    async def _try_get_forefront_request(self) -> Request | None:
        """Try to get the next available forefront request.

        Returns:
            The next forefront request or None if no forefront requests are available.
        """
        for request_id in list(self._forefront_requests):
            # Skip if already in progress
            if request_id in self._in_progress:
                continue

            request_path = self.path_to_rq / f'{request_id}.json'

            # Skip if file doesn't exist
            if not await asyncio.to_thread(request_path.exists):
                self._forefront_requests.remove(request_id)
                continue

            # Parse the request file
            request = await self._parse_request_file(request_path)

            # Skip if parsing failed
            if request is None:
                self._forefront_requests.remove(request_id)
                continue

            # Skip if already handled
            if request.handled_at is not None:
                self._forefront_requests.remove(request_id)
                continue

            # Mark as in-progress in memory
            self._in_progress.add(request.id)

            # Remove from forefront list
            self._forefront_requests.remove(request.id)

            # Update accessed timestamp
            await self._update_metadata(update_accessed_at=True)

            return request

        return None

    async def _categorize_regular_requests(self) -> tuple[dict[int, Path], list[Path]]:
        """Categorize regular (non-forefront) requests by sequence number.

        Returns:
            A tuple containing:
            - Dictionary mapping sequence numbers to request file paths
            - List of request file paths without sequence numbers
        """
        # List all request files for regular (non-forefront) requests
        request_files = await self._get_request_files()

        # Dictionary to store request files by their sequence number
        request_sequences = {}
        requests_without_sequence = []

        # Filter out metadata files and in-progress requests
        for request_file in request_files:
            # Extract request ID from filename
            request_id = request_file.stem

            # Skip if already in progress or in forefront
            if request_id in self._in_progress or request_id in self._forefront_requests:
                continue

            request = await self._parse_request_file(request_file)

            if request is None:
                continue

            # Skip if already handled
            if request.handled_at is not None:
                continue

            sequence_number = None if request.model_extra is None else request.model_extra.get('sequence')

            # If the request has a sequence number, add it to the dictionary
            if sequence_number:
                request_sequences[sequence_number] = request_file
            else:
                # If no sequence number, add to the list for ordering by file timestamp
                requests_without_sequence.append(request_file)

        return request_sequences, requests_without_sequence

    async def _try_get_sequenced_request(self, request_sequences: dict[int, Path]) -> Request | None:
        """Try to get the next request with a sequence number in FIFO order.

        Args:
            request_sequences: Dictionary mapping sequence numbers to request file paths

        Returns:
            The next sequenced request or None if no valid sequenced requests are available
        """
        # Process requests with sequence numbers first, in FIFO order
        for sequence in sorted(request_sequences.keys()):
            request_file = request_sequences[sequence]

            # Parse the request file
            request = await self._parse_request_file(request_file)

            # Skip if parsing failed
            if request is None:
                continue

            # Skip if already handled (double-check)
            if request.handled_at is not None:
                continue

            # Mark as in-progress in memory
            self._in_progress.add(request.id)

            # Update accessed timestamp
            await self._update_metadata(update_accessed_at=True)

            return request

        return None

    async def _try_get_non_sequenced_request(self, requests_without_sequence: list[Path]) -> Request | None:
        """Try to get the next request without a sequence number, using file timestamps for ordering.

        Args:
            requests_without_sequence: List of request file paths without sequence numbers

        Returns:
            The next non-sequenced request or None if no valid non-sequenced requests are available
        """
        if not requests_without_sequence:
            return None

        # Get file creation times for sorting
        request_file_times = {}
        for request_file in requests_without_sequence:
            try:
                file_stat = await asyncio.to_thread(request_file.stat)
                request_file_times[request_file] = file_stat.st_mtime
            except Exception:  # noqa: PERF203
                # If we can't get the time, use 0 (oldest)
                request_file_times[request_file] = 0

        # Sort by creation time
        requests_without_sequence.sort(key=lambda f: request_file_times[f])

        # Process requests without sequence in file timestamp order
        for request_file in requests_without_sequence:
            request = await self._parse_request_file(request_file)

            if request is None:
                continue

            if request.handled_at is not None:
                continue

            # Mark as in-progress in memory
            self._in_progress.add(request.id)

            # Update accessed timestamp
            await self._update_metadata(update_accessed_at=True)
            return request

        return None

    async def _get_request_files(self) -> list[Path]:
        """Get all request files in the queue.

        Returns:
            A list of paths to all request files in the queue.
        """
        # Create the requests directory if it doesn't exist.
        await asyncio.to_thread(self.path_to_rq.mkdir, parents=True, exist_ok=True)

        # List all the json files.
        files = await asyncio.to_thread(list, self.path_to_rq.glob('*.json'))

        # Filter out metadata file and non-file entries.
        filtered = filter(
            lambda request_file: request_file.is_file() and request_file.name != METADATA_FILENAME,
            files,
        )

        return list(filtered)

    async def _parse_request_file(self, file_path: Path) -> Request | None:
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
