from __future__ import annotations

import asyncio
import json
import shutil
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from pydantic import ValidationError
from typing_extensions import override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

from ._utils import METADATA_FILENAME, json_dumps

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class FileSystemRequestQueueClient(RequestQueueClient):
    """A file system implementation of the request queue client.

    This client persists requests to the file system, making it suitable for scenarios where data needs
    to survive process restarts. Each request is stored as a separate file, allowing for proper request
    handling and tracking across crawler runs.
    """

    _STORAGE_SUBDIR = 'request_queues'
    """The name of the subdirectory where request queues are stored."""

    _cache_by_name: ClassVar[dict[str, FileSystemRequestQueueClient]] = {}
    """A dictionary to cache clients by their names."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
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

    @override
    @property
    def metadata(self) -> RequestQueueMetadata:
        return self._metadata

    @property
    def path_to_rq(self) -> Path:
        """The full path to the request queue directory."""
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
        if id:
            raise ValueError(
                'Opening a dataset by "id" is not supported for file system storage client, use "name" instead.'
            )

        name = name or configuration.default_dataset_id

        # Check if the client is already cached by name.
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata(update_accessed_at=True)  # noqa: SLF001
            return client

        storage_dir = Path(configuration.storage_dir)
        rq_path = storage_dir / cls._STORAGE_SUBDIR / name
        metadata_path = rq_path / METADATA_FILENAME

        # If the RQ directory exists, reconstruct the client from the metadata file.
        if rq_path.exists():
            # If metadata file is missing, raise an error.
            if not metadata_path.exists():
                raise ValueError(f'Metadata file not found for RQ "{name}"')

            file = await asyncio.to_thread(open, metadata_path)
            try:
                file_content = json.load(file)
            finally:
                await asyncio.to_thread(file.close)
            try:
                metadata = RequestQueueMetadata(**file_content)
            except ValidationError as exc:
                raise ValueError(f'Invalid metadata file for RQ "{name}"') from exc

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

        # Cache the client by name.
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        # If the client directory exists, remove it recursively.
        if self.path_to_rq.exists():
            async with self._lock:
                await asyncio.to_thread(shutil.rmtree, self.path_to_rq)

        # Remove the client from the cache.
        if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
            del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

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
            processed_requests = []

            # Create the requests directory if it doesn't exist
            requests_dir = self.path_to_rq / 'requests'
            await asyncio.to_thread(requests_dir.mkdir, parents=True, exist_ok=True)

            # Create the in_progress directory if it doesn't exist
            in_progress_dir = self.path_to_rq / 'in_progress'
            await asyncio.to_thread(in_progress_dir.mkdir, parents=True, exist_ok=True)

            for request in requests:
                # Ensure the request has an ID
                if not request.id:
                    request.id = crypto_random_object_id()

                # Check if the request is already in the queue by unique_key
                existing_request = None

                # List all request files and check for matching unique_key
                request_files = await asyncio.to_thread(list, requests_dir.glob('*.json'))
                for request_file in request_files:
                    file = await asyncio.to_thread(open, request_file)
                    try:
                        file_content = json.load(file)
                        if file_content.get('unique_key') == request.unique_key:
                            existing_request = Request(**file_content)
                            break
                    except (json.JSONDecodeError, ValidationError):
                        logger.warning(f'Failed to parse request file: {request_file}')
                    finally:
                        await asyncio.to_thread(file.close)

                was_already_present = existing_request is not None
                was_already_handled = (
                    was_already_present and existing_request and existing_request.handled_at is not None
                )

                # If the request is already in the queue and handled, don't add it again
                if was_already_handled:
                    processed_requests.append(
                        ProcessedRequest(
                            id=request.id,
                            unique_key=request.unique_key,
                            was_already_present=True,
                            was_already_handled=True,
                        )
                    )
                    continue

                # If the request is already in the queue but not handled, update it
                if was_already_present:
                    # Update the existing request file
                    request_path = requests_dir / f'{request.id}.json'
                    request_data = await json_dumps(request.model_dump())
                    await asyncio.to_thread(request_path.write_text, request_data, encoding='utf-8')
                else:
                    # Add the new request to the queue
                    request_path = requests_dir / f'{request.id}.json'
                    request_data = await json_dumps(request.model_dump())
                    await asyncio.to_thread(request_path.write_text, request_data, encoding='utf-8')

                    # Update metadata counts
                    self._metadata.total_request_count += 1
                    self._metadata.pending_request_count += 1

                processed_requests.append(
                    ProcessedRequest(
                        id=request.id,
                        unique_key=request.unique_key,
                        was_already_present=was_already_present,
                        was_already_handled=False,
                    )
                )

            # Update metadata
            await self._update_metadata(update_modified_at=True)

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
        # First check in-progress directory
        in_progress_dir = self.path_to_rq / 'in_progress'
        in_progress_path = in_progress_dir / f'{request_id}.json'

        # Then check regular requests directory
        requests_dir = self.path_to_rq / 'requests'
        request_path = requests_dir / f'{request_id}.json'

        for path in [in_progress_path, request_path]:
            if await asyncio.to_thread(path.exists):
                file = await asyncio.to_thread(open, path)
                try:
                    file_content = json.load(file)
                    return Request(**file_content)
                except (json.JSONDecodeError, ValidationError) as e:
                    logger.warning(f'Failed to parse request file {path}: {e!s}')
                finally:
                    await asyncio.to_thread(file.close)

        return None

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
            # Create the requests and in_progress directories if they don't exist
            requests_dir = self.path_to_rq / 'requests'
            in_progress_dir = self.path_to_rq / 'in_progress'

            await asyncio.to_thread(requests_dir.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(in_progress_dir.mkdir, parents=True, exist_ok=True)

            # List all request files
            request_files = await asyncio.to_thread(list, requests_dir.glob('*.json'))

            # Find a request that's not handled
            for request_file in request_files:
                file = await asyncio.to_thread(open, request_file)
                try:
                    file_content = json.load(file)
                    # Skip if already handled
                    if file_content.get('handled_at') is not None:
                        continue

                    # Create request object
                    request = Request(**file_content)

                    # Move to in-progress
                    in_progress_path = in_progress_dir / f'{request.id}.json'

                    # If already in in-progress, skip
                    if await asyncio.to_thread(in_progress_path.exists):
                        continue

                    # Write to in-progress directory
                    request_data = await json_dumps(request.model_dump())
                    await asyncio.to_thread(in_progress_path.write_text, request_data, encoding='utf-8')

                except (json.JSONDecodeError, ValidationError) as e:
                    logger.warning(f'Failed to parse request file {request_file}: {e!s}')
                else:
                    return request
                finally:
                    await asyncio.to_thread(file.close)

            return None

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
            in_progress_dir = self.path_to_rq / 'in_progress'
            in_progress_path = in_progress_dir / f'{request.id}.json'

            if not await asyncio.to_thread(in_progress_path.exists):
                return None

            # Update the request object - set handled_at timestamp
            if request.handled_at is None:
                request.handled_at = datetime.now(timezone.utc)

            # Write the updated request back to the requests directory
            requests_dir = self.path_to_rq / 'requests'
            request_path = requests_dir / f'{request.id}.json'

            request_data = await json_dumps(request.model_dump())
            await asyncio.to_thread(request_path.write_text, request_data, encoding='utf-8')

            # Remove the in-progress file
            await asyncio.to_thread(in_progress_path.unlink, missing_ok=True)

            # Update metadata counts
            self._metadata.handled_request_count += 1
            self._metadata.pending_request_count -= 1

            # Update metadata timestamps
            await self._update_metadata(update_modified_at=True)

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
            in_progress_dir = self.path_to_rq / 'in_progress'
            in_progress_path = in_progress_dir / f'{request.id}.json'

            if not await asyncio.to_thread(in_progress_path.exists):
                return None

            # Remove the in-progress file
            await asyncio.to_thread(in_progress_path.unlink, missing_ok=True)

            # If forefront is true, we need to handle this specially
            # Since we can't reorder files, we'll add a 'priority' field to the request
            if forefront:
                # Update the priority of the request to indicate it should be processed first
                request.priority = 1  # Higher priority

            # Write the updated request back to the requests directory
            requests_dir = self.path_to_rq / 'requests'
            request_path = requests_dir / f'{request.id}.json'

            request_data = await json_dumps(request.model_dump())
            await asyncio.to_thread(request_path.write_text, request_data, encoding='utf-8')

            # Update metadata timestamps
            await self._update_metadata(update_modified_at=True)

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
        # Create the requests directory if it doesn't exist
        requests_dir = self.path_to_rq / 'requests'
        await asyncio.to_thread(requests_dir.mkdir, parents=True, exist_ok=True)

        # List all request files
        request_files = await asyncio.to_thread(list, requests_dir.glob('*.json'))

        # Check each file to see if there are any unhandled requests
        for request_file in request_files:
            file = await asyncio.to_thread(open, request_file)
            try:
                file_content = json.load(file)
                # If any request is not handled, the queue is not empty
                if file_content.get('handled_at') is None:
                    return False
            except (json.JSONDecodeError, ValidationError):
                logger.warning(f'Failed to parse request file: {request_file}')
            finally:
                await asyncio.to_thread(file.close)

        # If we got here, all requests are handled or there are no requests
        return True

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the dataset metadata file with current information.

        Args:
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now

        # Ensure the parent directory for the metadata file exists.
        await asyncio.to_thread(self.path_to_metadata.parent.mkdir, parents=True, exist_ok=True)

        # Dump the serialized metadata to the file.
        data = await json_dumps(self._metadata.model_dump())
        await asyncio.to_thread(self.path_to_metadata.write_text, data, encoding='utf-8')
