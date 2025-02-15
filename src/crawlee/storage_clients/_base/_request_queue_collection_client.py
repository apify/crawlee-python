from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from crawlee.storage_clients.models import RequestQueueListPage, RequestQueueMetadata


@docs_group('Abstract classes')
class RequestQueueCollectionClient(ABC):
    """An abstract class for request queue collection clients.

    This collection client handles operations that involve multiple instances of a given resource type.
    """

    @abstractmethod
    async def get_or_create(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        schema: dict | None = None,
    ) -> RequestQueueMetadata:
        """Retrieve an existing request queue by its name or ID, or create a new one if it does not exist.

        Args:
            id: Optional ID of the request queue to retrieve or create. If provided, the method will attempt
                to find a request queue with the ID.
            name: Optional name of the request queue resource to retrieve or create. If provided, the method will
                attempt to find a request queue with this name.
            schema: Optional schema for the request queue resource to be created.

        Returns:
            Metadata object containing the information of the retrieved or created request queue.
        """

    @abstractmethod
    async def list(
        self,
        *,
        unnamed: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool = False,
    ) -> RequestQueueListPage:
        """List the available request queues.

        Args:
            unnamed: Whether to list only the unnamed request queues.
            limit: Maximum number of request queues to return.
            offset: Number of request queues to skip from the beginning of the list.
            desc: Whether to sort the request queues in descending order.

        Returns:
            The list of available request queues matching the specified filters.
        """
