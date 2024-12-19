from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.request_loaders._request_loader import RequestLoader

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee._request import Request


@docs_group('Classes')
class RequestList(RequestLoader):
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
        self._name = name
        self._handled_count = 0

        self._requests = deque(self._transform_requests(requests or []))
        self._in_progress = set[str]()

    @property
    def name(self) -> str | None:
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
    async def fetch_next_request(self) -> Request | None:
        try:
            request = self._requests.popleft()
        except IndexError:
            return None
        else:
            self._in_progress.add(request.id)
            return request

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
