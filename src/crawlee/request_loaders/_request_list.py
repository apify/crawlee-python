from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.request_loaders._request_loader import RequestLoader

if TYPE_CHECKING:
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
        requests: Iterable[str | Request] | AsyncIterable[str | Request] | None = None,
        name: str | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            requests: The request objects (or their string representations) to be added to the provider.
            name: A name of the request list.
        """
        self._name = name
        self._handled_count = 0
        self._assumed_total_count = 0

        self._in_progress = set[str]()
        self._next: Request | None = None

        if isinstance(requests, AsyncIterable):
            self._requests = requests.__aiter__()
        elif requests is None:
            self._requests = self._iterate_in_threadpool([])
        else:
            self._requests = self._iterate_in_threadpool(requests)

        self._requests_lock: asyncio.Lock | None = None

    @property
    def name(self) -> str | None:
        return self._name

    @override
    async def get_total_count(self) -> int:
        return self._assumed_total_count

    @override
    async def is_empty(self) -> bool:
        await self._ensure_next_request()
        return self._next is None

    @override
    async def is_finished(self) -> bool:
        return len(self._in_progress) == 0 and await self.is_empty()

    @override
    async def fetch_next_request(self) -> Request | None:
        await self._ensure_next_request()

        if self._next is None:
            return None

        self._in_progress.add(self._next.id)
        self._assumed_total_count += 1

        next_request = self._next
        self._next = None

        return next_request

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        self._handled_count += 1
        self._in_progress.remove(request.id)

    @override
    async def get_handled_count(self) -> int:
        return self._handled_count

    async def _ensure_next_request(self) -> None:
        if self._requests_lock is None:
            self._requests_lock = asyncio.Lock()

        try:
            async with self._requests_lock:
                if self._next is None:
                    self._next = self._transform_request(await self._requests.__anext__())
        except StopAsyncIteration:
            self._next = None

    async def _iterate_in_threadpool(self, iterable: Iterable[str | Request]) -> AsyncIterator[str | Request]:
        """Inspired by a function of the same name from encode/starlette."""
        iterator = iter(iterable)

        class _StopIteration(Exception):  # noqa: N818
            pass

        def _next() -> str | Request:
            # We can't raise `StopIteration` from within the threadpool iterator
            # and catch it outside that context, so we coerce them into a different
            # exception type.
            try:
                return next(iterator)
            except StopIteration:
                raise _StopIteration  # noqa: B904

        try:
            while True:
                yield await asyncio.to_thread(_next)
        except _StopIteration:
            return
