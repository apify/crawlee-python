from __future__ import annotations

import itertools
import logging
from abc import ABC
from typing import TYPE_CHECKING, Any

from crawlee._utils import requests
from crawlee.events import Event, EventSystemInfoData
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import ProcessedRequest

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.storage_clients.models import AddRequestsResponse, RequestQueueMetadata


from cachetools import LRUCache

logger = logging.getLogger(__name__)


class RequestQueueClientCached(RequestQueueClient):
    """An abstract class for request queue resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """
    _add_request_cache: set[str] | None = None
    _MEMORY_USAGE_THRESHOLD = 0.95  # For cache reduction


    def __getattribute__(self, item: str) -> Any:
        """Redirect to cache aware methods for specific methods."""
        # This or add_batch_of_requests_direct and reclaim_request_direct new methods on the abstract class and wrap by default?
        if item == "add_batch_of_requests":
            return self.add_batch_of_requests_cached
        if item == "reclaim_request":
            return self.reclaim_request_cached
        return object.__getattribute__(self, item)

    async def add_batch_of_requests_cached(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
        deduplicate: bool = True,
    ) -> AddRequestsResponse:
        """Add batch of requests to the queue and to the local cache.

        This method adds a batch of requests to the queue. Each request is processed based on its uniqueness
        (determined by `unique_key`). Duplicates will be identified but not re-added to the queue.

        Args:
            requests: The collection of requests to add to the queue.
            forefront: Whether to put the added requests at the beginning (True) or the end (False) of the queue.
                When True, the requests will be processed sooner than previously added requests.
            batch_size: The maximum number of requests to add in a single batch.
            deduplicate: Ignore cached (previously added) requests from the batch.

        Returns:
            A response object containing information about which requests were successfully
            processed and which failed (if any).
        """

        # Should we cache full request or just unique key? Probably full as other attributes might be relevant
        alread_handled = []
        if deduplicate:
            new_requests = []
            for request in requests:
                if request.id not in self.add_request_cache:
                    new_requests.append(request)
                    self.add_request_cache.add(request.id)
                else:
                    alread_handled.append(ProcessedRequest(
                        id=request.id,
                        unique_key=request.unique_key,
                        was_already_present=True,  # This we know
                        was_already_handled=False,  # This we have no clue
                    ))

            _requests: Sequence[Request] = new_requests
            logger.warning(f"Deduplication: original: {len(requests)}, deduplicated: {len(_requests)}")
        else:
            _requests = requests
            self.add_request_cache.update({request.id for request in requests})

        response = await object.__getattribute__(self, "add_batch_of_requests")(requests=tuple(_requests), forefront=forefront)
        response.processed_requests.extend(alread_handled)
        return response

    async def reclaim_request_cached(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        """Reclaim a failed request back to the queue and remove it from cache.

        The request will be returned for processing later again by another call to `RequestQueue.fetch_next_request`.

        Args:
            request: The request to return to the queue.
            forefront: Whether to add the request to the head or the end of the queue.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        self.add_request_cache.remove(request.id)
        return await object.__getattribute__(self, "reclaim_request")(request=request, forefront=forefront)

    @property
    def add_request_cache(self):
        """Local."""
        if not self._add_request_cache:
            # To prevent circular imports
            from crawlee import service_locator
            # TBD dynamically reduce size if running out of memory. This is just optimization, so it should not block the execution
            self._add_request_cache = set[str]()
            service_locator.get_event_manager().on(event=Event.SYSTEM_INFO, listener=self._adjust_add_request_cache_size)
        return self._add_request_cache

    @add_request_cache.setter
    def add_request_cache(self, new_cache: set[str]) -> None:
        self._add_request_cache = new_cache

    async def _adjust_add_request_cache_size(self, event_data: EventSystemInfoData):
        """Reduce cache size if running out of memory."""
        return
        # TODO: Implement this properly
        limit_memory_size = (event_data.memory_info.total_size * self._MEMORY_USAGE_THRESHOLD)
        if event_data.memory_info.system_wide_used_size > limit_memory_size:
            # If running out of memory, drop half of the cache
            self.add_request_cache = set(itertools.islice(self.add_request_cache, len(self.add_request_cache) // 2))

