from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee.request_loaders import RequestManager
from crawlee.storage_clients.models import Request, RequestQueueMetadata

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import RequestQueueClient
    from crawlee.storage_clients.models import ProcessedRequest

logger = getLogger(__name__)

T = TypeVar('T')

# TODO: implement:
# - caching / memoization of both KVS & KVS clients

# Properties:
# - id
# - name
# - metadata

# Methods
# - open
# - drop
# - add_request
# - add_requests_batched
# - get_handled_count
# - get_total_count
# - get_request
# - fetch_next_request
# - mark_request_as_handled
# - reclaim_request
# - is_empty
# - is_finished

# Breaking changes:
# - from_storage_object method has been removed - Use the open method with name and/or id instead.
# - get_info -> metadata property
# - storage_object -> metadata property


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

    _MAX_CACHED_REQUESTS = 1_000_000
    """Maximum number of requests that can be cached."""

    def __init__(self, client: RequestQueueClient) -> None:
        """Initialize a new instance.

        Preferably use the `RequestQueue.open` constructor to create a new instance.

        Args:
            client: An instance of a key-value store client.
        """
        self._client = client

    @override
    @property
    def id(self) -> str:
        return self._client.id

    @override
    @property
    def name(self) -> str | None:
        return self._client.name

    @override
    @property
    def metadata(self) -> RequestQueueMetadata:
        return RequestQueueMetadata(
            id=self._client.id,
            name=self._client.id,
            accessed_at=self._client.accessed_at,
            created_at=self._client.created_at,
            modified_at=self._client.modified_at,
            had_multiple_clients=self._client.had_multiple_clients,
            handled_request_count=self._client.handled_request_count,
            pending_request_count=self._client.pending_request_count,
            stats=self._client.stats,
            total_request_count=self._client.total_request_count,
            resource_directory=self._client.resource_directory,
        )

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool | None = None,
        storage_dir: Path | None = None,
        configuration: Configuration | None = None,
        storage_client: StorageClient | None = None,
    ) -> RequestQueue:
        if id and name:
            raise ValueError('Only one of "id" or "name" can be specified, not both.')

        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client
        purge_on_start = configuration.purge_on_start if purge_on_start is None else purge_on_start
        storage_dir = Path(configuration.storage_dir) if storage_dir is None else storage_dir

        client = await storage_client.open_request_queue_client(
            id=id,
            name=name,
            purge_on_start=purge_on_start,
            storage_dir=storage_dir,
        )

        return cls(client)

    @override
    async def drop(self, *, timeout: timedelta | None = None) -> None:
        await self._client.drop()

    @override
    async def add_request(
        self,
        request: str | Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        return await self._client.add_request(request, forefront=forefront)

    @override
    async def add_requests_batched(
        self,
        requests: Sequence[str | Request],
        *,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        # TODO: implement
        pass

        # Wait for the first batch to be added
        first_batch = transformed_requests[:batch_size]
        if first_batch:
            await self._process_batch(first_batch, base_retry_wait=wait_time_between_batches)

        async def _process_remaining_batches() -> None:
            for i in range(batch_size, len(transformed_requests), batch_size):
                batch = transformed_requests[i : i + batch_size]
                await self._process_batch(batch, base_retry_wait=wait_time_between_batches)
                if i + batch_size < len(transformed_requests):
                    await asyncio.sleep(wait_time_secs)

        # Create and start the task to process remaining batches in the background
        remaining_batches_task = asyncio.create_task(
            _process_remaining_batches(), name='request_queue_process_remaining_batches_task'
        )
        self._tasks.append(remaining_batches_task)
        remaining_batches_task.add_done_callback(lambda _: self._tasks.remove(remaining_batches_task))

        # Wait for all tasks to finish if requested
        if wait_for_all_requests_to_be_added:
            await wait_for_all_tasks_for_finish(
                (remaining_batches_task,),
                logger=logger,
                timeout=wait_for_all_requests_to_be_added_timeout,
            )

    async def _process_batch(self, batch: Sequence[Request], base_retry_wait: timedelta, attempt: int = 1) -> None:
        max_attempts = 5
        response = await self._resource_client.batch_add_requests(batch)

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
        self._assumed_total_count += request_count
        if request_count:
            logger.debug(
                f'Added {request_count} requests to the queue. Processed requests: {response.processed_requests}'
            )

    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request from the queue.

        Args:
            request_id: ID of the request to retrieve.

        Returns:
            The retrieved request, or `None`, if it does not exist.
        """
        # TODO: implement

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
        # TODO: implement

    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after successful processing.

        Handled requests will never again be returned by the `RequestQueue.fetch_next_request` method.

        Args:
            request: The request to mark as handled.

        Returns:
            Information about the queue operation. `None` if the given request was not in progress.
        """
        # TODO: implement

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
        # TODO: implement

    async def is_empty(self) -> bool:
        """Check whether the queue is empty.

        Returns:
            `True` if the next call to `RequestQueue.fetch_next_request` would return `None`, otherwise `False`.
        """
        # TODO: implement

    async def is_finished(self) -> bool:
        """Check whether the queue is finished.

        Due to the nature of distributed storage used by the queue, the function might occasionally return a false
        negative, but it will never return a false positive.

        Returns:
            `True` if all requests were already handled and there are no more left. `False` otherwise.
        """
        # TODO: implement
