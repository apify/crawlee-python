from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee.request_loaders import RequestManager
from crawlee.storage_clients.models import ProcessedRequest, Request, RequestQueueMetadata

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import RequestQueueClient
    from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

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
            configuration=configuration,
        )

        return cls(client)

    @override
    async def drop(self) -> None:
        """Drop the request queue."""
        await self._client.drop()

    @override
    async def add_request(
        self,
        request: str | Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        """Add a request to the queue.

        Args:
            request: The request to add to the queue.
            forefront: Whether to add the request to the front of the queue.

        Returns:
            Information about the request operation.
        """
        request = self._transform_request(request)
        response = await self._client.add_requests([request], forefront=forefront)
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
    ) -> AddRequestsResponse:
        """Add multiple requests to the queue.

        Args:
            requests: The requests to add to the queue.
            forefront: Whether to add the requests to the front of the queue.
            batch_size: How many requests to send at once.
            wait_time_between_batches: How long to wait between batches.
            wait_for_all_requests_to_be_added: Whether to wait for all requests to be added.
            wait_for_all_requests_to_be_added_timeout: How long to wait for all requests to be added.

        Returns:
            Information about the batch operation.
        """
        transformed_requests = self._transform_requests(requests)

        return await self._client.add_requests(
            transformed_requests,
            forefront=forefront,
            batch_size=batch_size,
            wait_time_between_batches=wait_time_between_batches,
            wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
            wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
        )

    async def get_request(self, request_id: str) -> Request | None:
        """Retrieve a request by its ID.

        Args:
            request_id: The ID of the request to retrieve.

        Returns:
            The request if found, otherwise `None`.
        """
        return await self._client.get_request(request_id)

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
        """Check whether the queue is empty.

        Returns:
            `True` if the next call to `RequestQueue.fetch_next_request` would return `None`, otherwise `False`.
        """
        return await self._client.is_empty()

    async def is_finished(self) -> bool:
        """Check whether the queue is finished.

        Due to the nature of distributed storage used by the queue, the function might occasionally return a false
        negative, but it will never return a false positive.

        Returns:
            `True` if all requests were already handled and there are no more left. `False` otherwise.
        """
        return await self._client.is_finished()
