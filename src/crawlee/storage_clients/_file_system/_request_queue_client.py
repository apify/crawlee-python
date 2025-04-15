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

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import RequestQueueMetadata

from ._utils import METADATA_FILENAME, json_dumps

if TYPE_CHECKING:
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

    # TODO: other methods

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
