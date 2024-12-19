from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee import Request
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.base_storage_client._models import ProcessedRequest


@docs_group('Abstract classes')
class RequestLoader(ABC):
    """Abstract base class defining the interface for classes that provide access to a read-only stream of requests.

    Request loaders are used to manage and provide access to a storage of crawling requests.

    Key responsibilities:
        - Fetching the next request to be processed.
        - Marking requests as successfully handled after processing.
        - Managing state information such as the total and handled request counts.
    """

    @abstractmethod
    async def get_total_count(self) -> int:
        """Returns an offline approximation of the total number of requests in the source (i.e. pending + handled)."""

    @abstractmethod
    async def is_empty(self) -> bool:
        """Returns True if there are no more requests in the source (there might still be unfinished requests)."""

    @abstractmethod
    async def is_finished(self) -> bool:
        """Returns True if all requests have been handled."""

    @abstractmethod
    async def fetch_next_request(self) -> Request | None:
        """Returns the next request to be processed, or `null` if there are no more pending requests."""

    @abstractmethod
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Marks a request as handled after a successful processing (or after giving up retrying)."""

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