from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee import Request
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.base_storage_client._models import ProcessedRequest


@docs_group('Abstract classes')
class RequestSource(ABC):
    """Abstract base class defining the interface and common behaviour for request providers.

    Request providers are used to manage and provide access to a storage of crawling requests.

    Key responsibilities:
        - Fetching the next request to be processed.
        - Reclaiming requests that failed during processing, allowing retries.
        - Marking requests as successfully handled after processing.
        - Managing state information such as the total and handled request counts.

    Subclasses of `RequestSource` should provide specific implementations for each of the abstract methods.
    """

    @property
    @abstractmethod
    def name(self) -> str | None:
        """ID or name of the request queue."""

    @abstractmethod
    async def get_total_count(self) -> int:
        """Returns an offline approximation of the total number of requests in the queue (i.e. pending + handled)."""

    @abstractmethod
    async def is_empty(self) -> bool:
        """Returns True if there are no more requests in the queue (there might still be unfinished requests)."""

    @abstractmethod
    async def is_finished(self) -> bool:
        """Returns True if all requests have been handled."""

    @abstractmethod
    async def fetch_next_request(self) -> Request | None:
        """Returns a next request in the queue to be processed, or `null` if there are no more pending requests."""

    @abstractmethod
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Marks a request as handled after a successful processing (or after giving up retrying)."""

    @abstractmethod
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        """Reclaims a failed request back to the queue, so that it can be returned for processing later again.

        It is possible to modify the request data by supplying an updated request as a parameter.
        """

    @abstractmethod
    async def get_handled_count(self) -> int:
        """Returns the number of handled requests."""

    def _transform_request(self, request: str | Request) -> Request:
        """Transforms a request-like object into a Request object."""
        if isinstance(request, Request):
            return request

        if isinstance(request, str):
            return Request.from_url(request)

        raise ValueError(f'Invalid request type: {type(request)}')

    def _transform_requests(self, requests: Sequence[str | Request]) -> list[Request]:
        """Transforms a list of request-like objects into a list of `Request` objects."""
        processed_requests = dict[str, Request]()

        for request in requests:
            processed_request = self._transform_request(request)
            processed_requests.setdefault(processed_request.unique_key, processed_request)

        return list(processed_requests.values())
