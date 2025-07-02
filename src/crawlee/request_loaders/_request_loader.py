from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee import Request
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee.request_loaders import RequestManager, RequestManagerTandem
    from crawlee.storage_clients.models import ProcessedRequest


@docs_group('Abstract classes')
class RequestLoader(ABC):
    """An abstract class defining the interface for classes that provide access to a read-only stream of requests.

    Request loaders are used to manage and provide access to a storage of crawling requests.

    Key responsibilities:
        - Fetching the next request to be processed.
        - Marking requests as successfully handled after processing.
        - Managing state information such as the total and handled request counts.
    """

    @abstractmethod
    async def get_handled_count(self) -> int:
        """Get the number of requests in the loader that have been handled."""

    @abstractmethod
    async def get_total_count(self) -> int:
        """Get an offline approximation of the total number of requests in the loader (i.e. pending + handled)."""

    @abstractmethod
    async def is_empty(self) -> bool:
        """Return True if there are no more requests in the loader (there might still be unfinished requests)."""

    @abstractmethod
    async def is_finished(self) -> bool:
        """Return True if all requests have been handled."""

    @abstractmethod
    async def fetch_next_request(self) -> Request | None:
        """Return the next request to be processed, or `null` if there are no more pending requests."""

    @abstractmethod
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as handled after a successful processing (or after giving up retrying)."""

    async def to_tandem(self, request_manager: RequestManager | None = None) -> RequestManagerTandem:
        """Combine the loader with a request manager to support adding and reclaiming requests.

        Args:
            request_manager: Request manager to combine the loader with.
                If None is given, the default request queue is used.
        """
        # Import here to avoid circular imports.
        from crawlee.request_loaders import RequestManagerTandem  # noqa: PLC0415
        from crawlee.storages import RequestQueue  # noqa: PLC0415

        if request_manager is None:
            request_manager = await RequestQueue.open()

        return RequestManagerTandem(self, request_manager)

    def _transform_request(self, request: str | Request) -> Request:
        """Transform a request-like object into a Request object."""
        if isinstance(request, Request):
            return request

        if isinstance(request, str):
            return Request.from_url(request)

        raise ValueError(f'Invalid request type: {type(request)}')

    def _transform_requests(self, requests: Sequence[str | Request]) -> list[Request]:
        """Transform a list of request-like objects into a list of `Request` objects."""
        processed_requests = dict[str, Request]()

        for request in requests:
            processed_request = self._transform_request(request)
            processed_requests.setdefault(processed_request.unique_key, processed_request)

        return list(processed_requests.values())
