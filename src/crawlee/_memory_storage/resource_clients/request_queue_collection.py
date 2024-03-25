from __future__ import annotations

from typing import TYPE_CHECKING

from apify_shared.utils import ignore_docs

from apify._memory_storage.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from apify._memory_storage.resource_clients.request_queue import RequestQueueClient

if TYPE_CHECKING:
    from apify_shared.models import ListPage


@ignore_docs
class RequestQueueCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating request queues."""

    def _get_storage_client_cache(self: RequestQueueCollectionClient) -> list[RequestQueueClient]:
        return self._memory_storage_client._request_queues_handled

    def _get_resource_client_class(self: RequestQueueCollectionClient) -> type[RequestQueueClient]:
        return RequestQueueClient

    async def list(self: RequestQueueCollectionClient) -> ListPage:
        """List the available request queues.

        Returns:
            ListPage: The list of available request queues matching the specified filters.
        """
        return await super().list()

    async def get_or_create(
        self: RequestQueueCollectionClient,
        *,
        name: str | None = None,
        schema: dict | None = None,
        _id: str | None = None,
    ) -> dict:
        """Retrieve a named request queue, or create a new one when it doesn't exist.

        Args:
            name (str, optional): The name of the request queue to retrieve or create.
            schema (dict, optional): The schema of the request queue

        Returns:
            dict: The retrieved or newly-created request queue.
        """
        return await super().get_or_create(name=name, schema=schema, _id=_id)
