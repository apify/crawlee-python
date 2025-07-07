from __future__ import annotations

import asyncio
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import override

from crawlee import Request, service_locator
from crawlee._utils.docs import docs_group
from crawlee._utils.wait import wait_for_all_tasks_for_finish
from crawlee.request_loaders import RequestManager

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import RequestQueueClient
    from crawlee.storage_clients.models import ProcessedRequest, RequestQueueMetadata

logger = getLogger(__name__)

T = TypeVar('T')


@docs_group('Classes')
class RequestQueue(Storage, RequestManager):
    """Request queue is a storage for managing HTTP requests.

    The request queue class serves as a high-level interface for organizing and managing HTTP requests
    during web crawling. It provides methods for adding, retrieving, and manipulating requests throughout
    the crawling lifecycle, abstracting away the underlying storage implementation details.

    Request queue maintains the state of each URL to be crawled, tracking whether it has been processed,
    is currently being handled, or is waiting in the queue. Each URL in the queue is uniquely identified
    by a `unique_key` property, which prevents duplicate processing unless explicitly configured otherwise.

    The class supports both breadth-first and depth-first crawling strategies through its `forefront` parameter
    when adding requests. It also provides mechanisms for error handling and request reclamation when
    processing fails.

    You can open a request queue using the `open` class method, specifying either a name or ID to identify
    the queue. The underlying storage implementation is determined by the configured storage client.

    ### Usage

    ```python
    from crawlee.storages import RequestQueue

    # Open a request queue
    rq = await RequestQueue.open(name='my_queue')

    # Add a request
    await rq.add_request('https://example.com')

    # Process requests
    request = await rq.fetch_next_request()
    if request:
        try:
            # Process the request
            # ...
            await rq.mark_request_as_handled(request)
        except Exception:
            await rq.reclaim_request(request)
    ```
    """

    def __init__(self, client: RequestQueueClient, id: str, name: str | None) -> None:
        """Initialize a new instance.

        Preferably use the `RequestQueue.open` constructor to create a new instance.

        Args:
            client: An instance of a storage client.
            id: The unique identifier of the storage.
            name: The name of the storage, if available.
        """
        self._client = client
        self._id = id
        self._name = name

        self._add_requests_tasks = list[asyncio.Task]()
        """A list of tasks for adding requests to the queue."""

    @property
    @override
    def id(self) -> str:
        return self._id

    @property
    @override
    def name(self) -> str | None:
        return self._name

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        return await self._client.get_metadata()

    @override
    async def get_handled_count(self) -> int:
        metadata = await self._client.get_metadata()
        return metadata.handled_request_count

    @override
    async def get_total_count(self) -> int:
        metadata = await self._client.get_metadata()
        return metadata.total_request_count

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: StorageClient | None = None,
    ) -> RequestQueue:
        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client

        return await service_locator.storage_instance_manager.open_storage_instance(
            cls,
            id=id,
            name=name,
            configuration=configuration,
            client_opener=storage_client.create_rq_client,
        )

    @override
    async def drop(self) -> None:
        # Remove from cache before dropping
        storage_instance_manager = service_locator.storage_instance_manager
        storage_instance_manager.remove_from_cache(self)

        await self._client.drop()

    @override
    async def purge(self) -> None:
        await self._client.purge()

    @override
    async def add_request(
        self,
        request: str | Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        request = self._transform_request(request)
        response = await self._client.add_batch_of_requests([request], forefront=forefront)
        return response.processed_requests[0]

    @override
    async def add_requests(
        self,
        requests: Sequence[str | Request],
        *,
        forefront: bool = False,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        transformed_requests = self._transform_requests(requests)
        wait_time_secs = wait_time_between_batches.total_seconds()

        # Wait for the first batch to be added
        first_batch = transformed_requests[:batch_size]
        if first_batch:
            await self._process_batch(
                first_batch,
                base_retry_wait=wait_time_between_batches,
                forefront=forefront,
            )

        async def _process_remaining_batches() -> None:
            for i in range(batch_size, len(transformed_requests), batch_size):
                batch = transformed_requests[i : i + batch_size]
                await self._process_batch(
                    batch,
                    base_retry_wait=wait_time_between_batches,
                    forefront=forefront,
                )
                if i + batch_size < len(transformed_requests):
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

    async def fetch_next_request(self) -> Request | None:
        """Return the next request in the queue to be processed.

        Once you successfully finish processing of the request, you need to call `RequestQueue.mark_request_as_handled`
        to mark the request as handled in the queue. If there was some error in processing the request, call
        `RequestQueue.reclaim_request` instead, so that the queue will give the request to some other consumer
        in another call to the `fetch_next_request` method.

        Note that the `None` return value does not mean the queue processing finished, it means there are currently
        no pending requests. To check whether all requests in queue were finished, use `RequestQueue.is_finished`
        instead.

        Returns:
            The next request to process, or `None` if there are no more pending requests.
        """
        return await self._client.fetch_next_request()

    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a specific request from the queue by its ID.

        Args:
            request_id: The ID of the request to retrieve.

        Returns:
            The request with the specified ID, or `None` if no such request exists.
        """
        return await self._client.get_request(request_id)

    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        This method should be called after a request has been successfully processed.
        Once marked as handled, the request will be removed from the queue and will
        not be returned in subsequent calls to `fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation.
        """
        return await self._client.mark_request_as_handled(request)

    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        """Reclaim a failed request back to the queue for later processing.

        If a request fails during processing, this method can be used to return it to the queue.
        The request will be returned for processing again in a subsequent call
        to `RequestQueue.fetch_next_request`.

        Args:
            request: The request to return to the queue.
            forefront: If true, the request will be added to the beginning of the queue.
                Otherwise, it will be added to the end.

        Returns:
            Information about the queue operation.
        """
        return await self._client.reclaim_request(request, forefront=forefront)

    async def is_empty(self) -> bool:
        """Check if the request queue is empty.

        An empty queue means that there are no requests currently in the queue, either pending or being processed.
        However, this does not necessarily mean that the crawling operation is finished, as there still might be
        tasks that could add additional requests to the queue.

        Returns:
            True if the request queue is empty, False otherwise.
        """
        return await self._client.is_empty()

    async def is_finished(self) -> bool:
        """Check if the request queue is finished.

        A finished queue means that all requests in the queue have been processed (the queue is empty) and there
        are no more tasks that could add additional requests to the queue. This is the definitive way to check
        if a crawling operation is complete.

        Returns:
            True if the request queue is finished (empty and no pending add operations), False otherwise.
        """
        if self._add_requests_tasks:
            logger.debug('Background add requests tasks are still in progress.')
            return False

        if await self.is_empty():
            logger.debug('The request queue is empty.')
            return True

        return False

    async def _process_batch(
        self,
        batch: Sequence[Request],
        *,
        base_retry_wait: timedelta,
        attempt: int = 1,
        forefront: bool = False,
    ) -> None:
        """Process a batch of requests with automatic retry mechanism."""
        max_attempts = 5
        response = await self._client.add_batch_of_requests(batch, forefront=forefront)

        if response.unprocessed_requests:
            logger.debug(f'Following requests were not processed: {response.unprocessed_requests}.')
            if attempt > max_attempts:
                logger.warning(
                    f'Following requests were not processed even after {max_attempts} attempts:\n'
                    f'{response.unprocessed_requests}'
                )
            else:
                logger.debug('Retry to add requests.')
                unprocessed_requests_unique_keys = {request.unique_key for request in response.unprocessed_requests}
                retry_batch = [request for request in batch if request.unique_key in unprocessed_requests_unique_keys]
                await asyncio.sleep((base_retry_wait * attempt).total_seconds())
                await self._process_batch(retry_batch, base_retry_wait=base_retry_wait, attempt=attempt + 1)

        request_count = len(batch) - len(response.unprocessed_requests)

        if request_count:
            logger.debug(
                f'Added {request_count} requests to the queue. Processed requests: {response.processed_requests}'
            )
