from __future__ import annotations

import asyncio
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar

from apify_client import ApifyClientAsync
from typing_extensions import override

from crawlee import Request
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import (
    AddRequestsResponse,
    ProcessedRequest,
    ProlongRequestLockResponse,
    Request,
    RequestQueueHead,
    RequestQueueMetadata,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from apify_client.clients import RequestQueueClientAsync

    from crawlee.configuration import Configuration

logger = getLogger(__name__)

_API_TOKEN = 'apify_api_Z9PfLfYya1llJGUlunDN15YVl4uo8r40hVCV'
_API_URL = 'https://api.apify.com'


class ApifyRequestQueueClient(RequestQueueClient):
    """An Apify platform implementation of the request queue client."""

    _cache_by_name: ClassVar[dict[str, ApifyRequestQueueClient]] = {}
    """A dictionary to cache clients by their names."""

    _DEFAULT_LOCK_TIME = timedelta(minutes=3)
    """The default lock time for requests in the queue."""

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
        api_client: RequestQueueClientAsync,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `ApifyRequestQueueClient.open` class method to create a new instance.
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

        self._api_client = api_client
        """The Apify key-value store client for API operations."""

        self._lock = asyncio.Lock()
        """A lock to ensure that only one operation is performed at a time."""

        self._add_requests_tasks = list[asyncio.Task]()
        """A list of tasks for adding requests to the queue."""

        self._assumed_total_count = 0
        """An assumed total count of requests in the queue."""

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
    ) -> ApifyRequestQueueClient:
        default_name = configuration.default_request_queue_id
        token = _API_TOKEN  # TODO: use the real value
        api_url = _API_URL  # TODO: use the real value

        name = name or default_name

        # Check if the client is already cached by name.
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata()  # noqa: SLF001
            return client

        # Otherwise, create a new one.
        apify_client_async = ApifyClientAsync(
            token=token,
            api_url=api_url,
            max_retries=8,
            min_delay_between_retries_millis=500,
            timeout_secs=360,
        )

        apify_rqs_client = apify_client_async.request_queues()

        metadata = RequestQueueMetadata.model_validate(
            await apify_rqs_client.get_or_create(name=id if id is not None else name),
        )

        apify_rq_client = apify_client_async.request_queue(request_queue_id=metadata.id)

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
            api_client=apify_rq_client,
        )

        # Cache the client by name.
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        async with self._lock:
            await self._api_client.delete()

            # Remove the client from the cache.
            if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
                del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

    @override
    async def list_head(
        self,
        *,
        lock_time: timedelta | None = None,
        limit: int | None = None,
    ) -> RequestQueueHead:
        lock_time = lock_time or self._DEFAULT_LOCK_TIME

        response = await self._api_client.list_and_lock_head(
            lock_secs=int(lock_time.total_seconds()),
            limit=limit,
        )

        return RequestQueueHead.model_validate(**response)

    @override
    async def add_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> AddRequestsResponse:
        wait_time_secs = wait_time_between_batches.total_seconds()

        async def _process_batch(batch: Sequence[Request]) -> None:
            request_count = len(batch)
            requests_dict = [request.model_dump(by_alias=True) for request in batch]
            response = await self._api_client.batch_add_requests(requests=requests_dict, forefront=forefront)
            self._assumed_total_count += request_count
            logger.debug(f'Added {request_count} requests to the queue, response: {response}')

        # Wait for the first batch to be added
        first_batch = requests[:batch_size]
        if first_batch:
            await _process_batch(first_batch)

        async def _process_remaining_batches() -> None:
            for i in range(batch_size, len(requests), batch_size):
                batch = requests[i : i + batch_size]
                await _process_batch(batch)
                if i + batch_size < len(requests):
                    await asyncio.sleep(wait_time_secs)

        # Create and start the task to process remaining batches in the background
        remaining_batches_task = asyncio.create_task(
            _process_remaining_batches(),
            name='request_queue_process_remaining_batches_task',
        )
        self._add_requests_tasks.append(remaining_batches_task)
        remaining_batches_task.add_done_callback(lambda _: self._add_requests_tasks.remove(remaining_batches_task))

        # Wait for all tasks to finish if requested
        if wait_for_all_requests_to_be_added:
            await wait_for_all_tasks_for_finish(
                (remaining_batches_task,),
                logger=logger,
                timeout=wait_for_all_requests_to_be_added_timeout,
            )

        response = await self._api_client.batch_add_requests(
            requests=[request.model_dump(by_alias=True, exclude={'id'}) for request in requests],
            forefront=forefront,
        )
        return AddRequestsResponse.model_validate(response)

    @override
    async def get_request(self, request_id: str) -> Request | None:
        response = await self._api_client.get_request(request_id)
        if response is None:
            return None
        return Request.model_validate(**response)

    @override
    async def update_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        response = await self._api_client.update_request(
            request=request.model_dump(by_alias=True),
            forefront=forefront,
        )

        return ProcessedRequest.model_validate(
            {'id': request.id, 'uniqueKey': request.unique_key} | response,
        )

    @override
    async def is_finished(self) -> bool:
        if self._add_requests_tasks:
            logger.debug('Background tasks are still in progress')
            return False

        # TODO

    async def _prolong_request_lock(
        self,
        request_id: str,
        *,
        forefront: bool = False,
        lock_secs: int,
    ) -> ProlongRequestLockResponse:
        """Prolong the lock on a specific request in the queue.

        Args:
            request_id: The identifier of the request whose lock is to be prolonged.
            forefront: Whether to put the request in the beginning or the end of the queue after lock expires.
            lock_secs: The additional amount of time, in seconds, that the request will remain locked.
        """

    async def _delete_request_lock(
        self,
        request_id: str,
        *,
        forefront: bool = False,
    ) -> None:
        """Delete the lock on a specific request in the queue.

        Args:
            request_id: ID of the request to delete the lock.
            forefront: Whether to put the request in the beginning or the end of the queue after the lock is deleted.
        """

    async def _update_metadata(self) -> None:
        """Update the request queue metadata with current information."""
        metadata = await self._api_client.get()
        self._metadata = RequestQueueMetadata.model_validate(metadata)
