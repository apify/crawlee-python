from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from logging import getLogger
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import override

from crawlee._request import Request
from crawlee._utils.docs import docs_group
from crawlee.request_loaders._request_loader import RequestLoader

logger = getLogger(__name__)


class RequestListState(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    next_index: Annotated[int, Field(alias='nextIndex')] = 0
    next_unique_key: Annotated[str | None, Field(alias='nextUniqueKey')] = None
    in_progress: Annotated[set[str], Field(alias='inProgress')] = set()


class RequestListData(BaseModel):
    requests: Annotated[list[Request], Field()]


@docs_group('Request loaders')
class RequestList(RequestLoader):
    """Represents a (potentially very large) list of URLs to crawl."""

    def __init__(
        self,
        requests: Iterable[str | Request] | AsyncIterable[str | Request] | None = None,
        name: str | None = None,
        persist_state_key: str | None = None,
        persist_requests_key: str | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            requests: The request objects (or their string representations) to be added to the provider.
            name: A name of the request list.
            persist_state_key: A key for persisting the progress information of the RequestList.
                If you do not pass a key but pass a `name`, a key will be derived using the name.
                Otherwise, state will not be persisted.
            persist_requests_key: A key for persisting the request data loaded from the `requests` iterator.
                If specified, the request data will be stored in the KeyValueStore to make sure that they don't change
                over time. This is useful if the `requests` iterator pulls the data dynamically.
        """
        from crawlee._utils.recoverable_state import RecoverableState  # noqa: PLC0415

        self._name = name
        self._handled_count = 0
        self._assumed_total_count = 0

        self._next: tuple[Request | None, Request | None] = (None, None)

        if persist_state_key is None and name is not None:
            persist_state_key = f'SDK_REQUEST_LIST_STATE-{name}'

        self._state = RecoverableState(
            default_state=RequestListState(),
            persistence_enabled=bool(persist_state_key),
            persist_state_key=persist_state_key or '',
            logger=logger,
        )

        self._persist_request_data = bool(persist_requests_key)

        self._requests_data = RecoverableState(
            default_state=RequestListData(requests=[]),
            # With request data persistence enabled, a snapshot of the requests will be done on initialization
            persistence_enabled='explicit_only' if self._persist_request_data else False,
            persist_state_key=persist_requests_key or '',
            logger=logger,
        )

        if isinstance(requests, AsyncIterable):
            self._requests = requests.__aiter__()
        elif requests is None:
            self._requests = self._iterate_in_threadpool([])
        else:
            self._requests = self._iterate_in_threadpool(requests)

        self._requests_lock: asyncio.Lock | None = None

    async def _get_state(self) -> RequestListState:
        # If state is already initialized, we are done
        if self._state.is_initialized:
            return self._state.current_value

        # Initialize recoverable state
        await self._state.initialize()
        await self._requests_data.initialize()

        # Initialize lock if necessary
        if self._requests_lock is None:
            self._requests_lock = asyncio.Lock()

        # If the RequestList is configured to persist request data, ensure that a copy of request data is used
        if self._persist_request_data:
            async with self._requests_lock:
                if not await self._requests_data.has_persisted_state():
                    self._requests_data.current_value.requests = [
                        request if isinstance(request, Request) else Request.from_url(request)
                        async for request in self._requests
                    ]
                    await self._requests_data.persist_state()

                self._requests = self._iterate_in_threadpool(
                    self._requests_data.current_value.requests[self._state.current_value.next_index :]
                )
        # If not using persistent request data, advance the request iterator
        else:
            async with self._requests_lock:
                for _ in range(self._state.current_value.next_index):
                    with contextlib.suppress(StopAsyncIteration):
                        await self._requests.__anext__()

        # Check consistency of the stored state and the request iterator
        if (unique_key_to_check := self._state.current_value.next_unique_key) is not None:
            await self._ensure_next_request()

            next_unique_key = self._next[0].unique_key if self._next[0] is not None else None
            if next_unique_key != unique_key_to_check:
                raise RuntimeError(
                    f"""Mismatch at index {
                        self._state.current_value.next_index
                    } in persisted requests - Expected unique key `{unique_key_to_check}`, got `{next_unique_key}`"""
                )

        return self._state.current_value

    @property
    def name(self) -> str | None:
        return self._name

    @override
    async def get_handled_count(self) -> int:
        return self._handled_count

    @override
    async def get_total_count(self) -> int:
        return self._assumed_total_count

    @override
    async def is_empty(self) -> bool:
        await self._ensure_next_request()
        return self._next[0] is None

    @override
    async def is_finished(self) -> bool:
        state = await self._get_state()
        return len(state.in_progress) == 0 and await self.is_empty()

    @override
    async def fetch_next_request(self) -> Request | None:
        await self._get_state()
        await self._ensure_next_request()

        if self._next[0] is None:
            return None

        state = await self._get_state()
        state.in_progress.add(self._next[0].unique_key)
        self._assumed_total_count += 1

        next_request = self._next[0]
        if next_request is not None:
            state.next_index += 1
            state.next_unique_key = self._next[1].unique_key if self._next[1] is not None else None

        self._next = (self._next[1], None)
        await self._ensure_next_request()

        return next_request

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        self._handled_count += 1
        state = await self._get_state()
        state.in_progress.remove(request.unique_key)

    async def _ensure_next_request(self) -> None:
        await self._get_state()

        if self._requests_lock is None:
            self._requests_lock = asyncio.Lock()

        async with self._requests_lock:
            if None in self._next:
                if self._next[0] is None:
                    to_enqueue = [item async for item in self._dequeue_requests(2)]
                    self._next = (to_enqueue[0], to_enqueue[1])
                else:
                    to_enqueue = [item async for item in self._dequeue_requests(1)]
                    self._next = (self._next[0], to_enqueue[0])

    async def _dequeue_requests(self, count: int) -> AsyncGenerator[Request | None]:
        for _ in range(count):
            try:
                yield self._transform_request(await self._requests.__anext__())
            except StopAsyncIteration:  # noqa: PERF203
                yield None

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
