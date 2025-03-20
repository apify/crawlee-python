from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee.request_loaders._request_loader import RequestLoader

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee._request import Request
    from crawlee.storage_clients.models import ProcessedRequest


@docs_group('Abstract classes')
class RequestManager(RequestLoader, ABC):
    """Base class that extends `RequestLoader` with the capability to enqueue new requests and reclaim failed ones."""

    @abstractmethod
    async def drop(self) -> None:
        """Remove persistent state either from the Apify Cloud storage or from the local database."""

    @abstractmethod
    async def add_request(
        self,
        request: str | Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        """Add a single request to the manager and store it in underlying resource client.

        Args:
            request: The request object (or its string representation) to be added to the manager.
            forefront: Determines whether the request should be added to the beginning (if True) or the end (if False)
                of the manager.

        Returns:
            Information about the request addition to the manager.
        """

    async def add_requests_batched(
        self,
        requests: Sequence[str | Request],
        *,
        batch_size: int = 1000,  # noqa: ARG002
        wait_time_between_batches: timedelta = timedelta(seconds=1),  # noqa: ARG002
        wait_for_all_requests_to_be_added: bool = False,  # noqa: ARG002
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,  # noqa: ARG002
    ) -> None:
        """Add requests to the manager in batches.

        Args:
            requests: Requests to enqueue.
            batch_size: The number of requests to add in one batch.
            wait_time_between_batches: Time to wait between adding batches.
            wait_for_all_requests_to_be_added: If True, wait for all requests to be added before returning.
            wait_for_all_requests_to_be_added_timeout: Timeout for waiting for all requests to be added.
        """
        # Default and dumb implementation.
        for request in requests:
            await self.add_request(request)

    @abstractmethod
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        """Reclaims a failed request back to the source, so that it can be returned for processing later again.

        It is possible to modify the request data by supplying an updated request as a parameter.
        """
