from __future__ import annotations

from collections import deque
from datetime import timedelta
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.base_storage_client._models import ProcessedRequest
from crawlee.storages._request_provider import RequestProvider

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee._request import Request


@docs_group('Classes')
class RequestList(RequestProvider):
    """Represents a (potentially very large) list of URLs to crawl.

    Disclaimer: The `RequestList` class is in its early version and is not fully implemented. It is currently
        intended mainly for testing purposes and small-scale projects. The current implementation is only in-memory
        storage and is very limited. It will be (re)implemented in the future. For more details, see the GitHub issue:
        https://github.com/apify/crawlee-python/issues/99. For production usage we recommend to use the `RequestQueue`.
    """

    def __init__(
        self,
        requests: Sequence[str | Request] | None = None,
        name: str | None = None,
    ) -> None:
        """A default constructor.

        Args:
            requests: The request objects (or their string representations) to be added to the provider.
            name: A name of the request list.
        """
        self._name = name or ''
        self._handled_count = 0

        self._requests = deque(self._transform_requests(requests or []))
        self._in_progress = set[str]()

    @property
    @override
    def name(self) -> str:
        return self._name

    @override
    async def get_total_count(self) -> int:
        return len(self._requests)

    @override
    async def is_empty(self) -> bool:
        return len(self._requests) == 0

    @override
    async def is_finished(self) -> bool:
        return await self.is_empty() and len(self._in_progress) == 0

    @override
    async def drop(self) -> None:
        self._requests.clear()

    @override
    async def fetch_next_request(self) -> Request | None:
        try:
            request = self._requests.popleft()
        except IndexError:
            return None
        else:
            self._in_progress.add(request.id)
            return request

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> None:
        if forefront:
            self._requests.appendleft(request)
        else:
            self._requests.append(request)

        self._in_progress.remove(request.id)

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        self._handled_count += 1
        self._in_progress.remove(request.id)

    @override
    async def get_handled_count(self) -> int:
        return self._handled_count

    @override
    async def add_request(
        self,
        request: str | Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest:
        request = self._transform_request(request)

        if forefront:
            self._requests.appendleft(request)
        else:
            self._requests.append(request)

        return ProcessedRequest(
            id=request.id,
            unique_key=request.unique_key,
            was_already_handled=False,
            was_already_present=False,
        )

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
        transformed_requests = self._transform_requests(requests)
        self._requests.extend(transformed_requests)
