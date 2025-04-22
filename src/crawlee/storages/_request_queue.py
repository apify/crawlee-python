from __future__ import annotations

import asyncio
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar, TypeVar

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
    """Represents a queue storage for managing HTTP requests in web crawling operations.

    The `RequestQueue` class handles a queue of HTTP requests, each identified by a unique URL, to facilitate structured
    web crawling. It supports both breadth-first and depth-first crawling strategies, allowing for recursive crawling
    starting from an initial set of URLs. Each URL in the queue is uniquely identified by a `unique_key`, which can be
    customized to allow the same URL to be added multiple times under different keys.

    Data can be stored either locally or in the cloud. It depends on the setup of underlying storage client.
    By default a `MemoryStorageClient` is used, but it can be changed to a different one.

    By default, data is stored using the following path structure:
    ```
    {CRAWLEE_STORAGE_DIR}/request_queues/{QUEUE_ID}/{REQUEST_ID}.json
    ```
    - `{CRAWLEE_STORAGE_DIR}`: The root directory for all storage data specified by the environment variable.
    - `{QUEUE_ID}`: The identifier for the request queue, either "default" or as specified.
    - `{REQUEST_ID}`: The unique identifier for each request in the queue.

    The `RequestQueue` supports both creating new queues and opening existing ones by `id` or `name`. Named queues
    persist indefinitely, while unnamed queues expire after 7 days unless specified otherwise. The queue supports
    mutable operations, allowing URLs to be added and removed as needed.

    ### Usage

    ```python
    from crawlee.storages import RequestQueue

    rq = await RequestQueue.open(name='my_rq')
    ```
    """

    _cache_by_id: ClassVar[dict[str, RequestQueue]] = {}
    """A dictionary to cache request queues by their IDs."""

    _cache_by_name: ClassVar[dict[str, RequestQueue]] = {}
    """A dictionary to cache request queues by their names."""

    _MAX_CACHED_REQUESTS = 1_000_000
    """Maximum number of requests that can be cached."""

    def __init__(self, client: RequestQueueClient) -> None:
        """Initialize a new instance.

        Preferably use the `RequestQueue.open` constructor to create a new instance.

        Args:
            client: An instance of a request queue client.
        """
        self._client = client

        self._add_requests_tasks = list[asyncio.Task]()
        """A list of tasks for adding requests to the queue."""

    @override
    @property
    def id(self) -> str:
        return self._client.metadata.id

    @override
    @property
    def name(self) -> str | None:
        return self._client.metadata.name

    @override
    @property
    def metadata(self) -> RequestQueueMetadata:
        return self._client.metadata

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
        if id and name:
            raise ValueError('Only one of "id" or "name" can be specified, not both.')

        # Check if request queue is already cached by id or name
        if id and id in cls._cache_by_id:
            return cls._cache_by_id[id]
        if name and name in cls._cache_by_name:
            return cls._cache_by_name[name]

        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client

        client = await storage_client.open_request_queue_client(
            id=id,
            name=name,
            configuration=configuration,
        )

        rq = cls(client)

        # Cache the request queue by id and name if available
        if rq.id:
            cls._cache_by_id[rq.id] = rq
        if rq.name:
            cls._cache_by_name[rq.name] = rq

        return rq

    @override
    async def drop(self) -> None:
        # Remove from cache before dropping
        if self.id in self._cache_by_id:
            del self._cache_by_id[self.id]
        if self.name and self.name in self._cache_by_name:
            del self._cache_by_name[self.name]

        await self._client.drop()

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
            The request or `None` if there are no more pending requests.
        """
        return await self._client.fetch_next_request()

    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request by its ID.

        Args:
            request_id: The ID of the request to retrieve.

        Returns:
            The request if found, otherwise `None`.
        """
        return await self._client.get_request(request_id)

    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `RequestQueue.fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        return await self._client.mark_request_as_handled(request)

    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        """Reclaim a failed request back to the queue.

        The request will be returned for processing later again by another call to `RequestQueue.fetch_next_request`.

        Args:
            request: The request to return to the queue.
            forefront: Whether to add the request to the head or the end of the queue.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        return await self._client.reclaim_request(request, forefront=forefront)

    async def is_empty(self) -> bool:
        """Check if the request queue is empty.

        An empty queue means that there are no requests in the queue.

        Returns:
            True if the request queue is empty, False otherwise.
        """
        return await self._client.is_empty()

    async def is_finished(self) -> bool:
        """Check if the request queue is finished.

        Finished means that all requests in the queue have been processed (the queue is empty) and there
        are no more tasks that could add additional requests to the queue.

        Returns:
            True if the request queue is finished, False otherwise.
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
