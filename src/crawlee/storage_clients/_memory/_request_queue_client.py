from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar

from typing_extensions import override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import RequestQueueMetadata

if TYPE_CHECKING:
    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class MemoryRequestQueueClient(RequestQueueClient):
    """A memory implementation of the request queue client.

    This client stores requests in memory using a list. No data is persisted, which means
    all requests are lost when the process terminates. This implementation is mainly useful
    for testing and development purposes where persistence is not required.
    """

    _cache_by_name: ClassVar[dict[str, MemoryRequestQueueClient]] = {}
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

        # List to hold RQ items
        self._records = list[Request]()

    @override
    @property
    def metadata(self) -> RequestQueueMetadata:
        return self._metadata

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> MemoryRequestQueueClient:
        name = name or configuration.default_request_queue_id

        # Check if the client is already cached by name
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata(update_accessed_at=True)  # noqa: SLF001
            return client

        # If specific id is provided, use it; otherwise, generate a new one
        id = id or crypto_random_object_id()
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
        )

        # Cache the client by name
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        # Clear all data
        self._records.clear()

        # Remove from cache
        if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
            del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

    # TODO: other methods

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the request queue metadata with current information.

        Args:
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now
