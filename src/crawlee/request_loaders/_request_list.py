from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from logging import getLogger
from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.alias_generators import to_camel
from typing_extensions import override

from crawlee._request import Request
from crawlee._utils.docs import docs_group
from crawlee.request_loaders._request_loader import RequestLoader

from ._types import FinishedStatus, ReadyStatus, WaitingStatus

if TYPE_CHECKING:
    from ._types import RequestLoaderStatus

logger = getLogger(__name__)


class RequestListState(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True, validate_by_alias=True)

    next_index: Annotated[int, Field(ge=0)] = 0
    next_unique_key: str | None = None
    in_progress: set[str] = set()


class RequestListData(BaseModel):
    requests: Annotated[list[Request], Field()]


@docs_group('Request loaders')
class RequestList(RequestLoader):
    """Represents a (potentially very large) list of URLs to crawl."""

    def __init__(
        self,
        requests: Iterable[str | Request] | AsyncIterable[str | Request] | None = None,
        *,
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

        self._next: tuple[Request | None, Request | None] = (None, None)
        self._requests_to_retry = deque[Request]()

        if persist_state_key is None and name is not None:
            persist_state_key = f'SDK_REQUEST_LIST_STATE-{name}'

        self._state = RecoverableState(
            default_state=RequestListState(),
            persistence_enabled=bool(persist_state_key),
            persist_state_key=persist_state_key or '',
            logger=logger,
        )

        self._persist_requests_key = persist_requests_key

        self._requests: AsyncIterator[str | Request]
        if isinstance(requests, AsyncIterable):
            self._requests = requests.__aiter__()
        elif requests is None:
            self._requests = self._iterate_in_threadpool([])
        else:
            self._requests = self._iterate_in_threadpool(requests)

        self._requests_lock: asyncio.Lock | None = None
        self._initialized = False
        self._initialize_lock = asyncio.Lock()

    async def _get_state(self) -> RequestListState:
        if not self._initialized:
            async with self._initialize_lock:
                if not self._initialized:
                    await self._initialize()
                    self._initialized = True

        return self._state.current_value

    async def _initialize(self) -> None:
        from crawlee.storages import KeyValueStore  # noqa: PLC0415

        state = await self._state.initialize()
        requests_to_persist: RequestListData | None = None

        # If the RequestList is configured to persist request data, ensure that a copy of request data is used
        if self._persist_requests_key:
            key_value_store = await KeyValueStore.open()
            persisted_requests = await key_value_store.get_value(self._persist_requests_key)

            if persisted_requests is None:
                requests_data = RequestListData(requests=[])
                async for processing_request in self._requests:
                    try:
                        request = self._transform_request(processing_request)
                    except ValidationError:
                        logger.warning('Invalid request encountered in the request list: %s', processing_request)
                        continue
                    requests_data.requests.append(request)
                requests_to_persist = requests_data
            else:
                requests_data = RequestListData.model_validate(persisted_requests)

            fetched_requests = requests_data.requests[: state.next_index]
            self._requests = self._iterate_in_threadpool(requests_data.requests[state.next_index :])
        # If not using persistent request data, advance the request iterator
        else:
            # Only the in-progress requests are kept, so that skipping a large source doesn't hold all of it in memory.
            fetched_requests = [
                request
                async for request in self._dequeue_requests(state.next_index)
                if request is not None and request.unique_key in state.in_progress
            ]

        # Check consistency of the stored state and the request iterator
        if (unique_key_to_check := state.next_unique_key) is not None:
            await self._ensure_next_request()

            next_unique_key = self._next[0].unique_key if self._next[0] is not None else None
            if next_unique_key != unique_key_to_check:
                raise RuntimeError(
                    f"""Mismatch at index {state.next_index} in persisted requests - Expected unique key `{
                        unique_key_to_check
                    }`, got `{next_unique_key}`"""
                )

        # Persisted only after the check above, so that a mismatch doesn't persist requests that the saved position
        # doesn't fit.
        if self._persist_requests_key and requests_to_persist is not None:
            key_value_store = await KeyValueStore.open()
            await key_value_store.set_value(
                self._persist_requests_key,
                requests_to_persist.model_dump(mode='json', by_alias=True),
                content_type='application/json',
            )

        # The requests that were in progress when the state was persisted are served again first. An in-progress key
        # missing among the fetched requests can't be served again, so it's dropped: it's either ahead of `next_index`,
        # to be fetched anyway, or unknown. Dropping waits for the check above, so a mismatch leaves the record intact.
        requests_to_retry = {
            request.unique_key: request for request in fetched_requests if request.unique_key in state.in_progress
        }
        if dropped_keys := state.in_progress.difference(requests_to_retry):
            logger.warning(
                "RequestList's in-progress field is not consistent, skipping invalid in-progress entries",
                extra={'delete_from_in_progress': sorted(dropped_keys)},
            )
        self._requests_to_retry.extend(requests_to_retry.values())
        state.in_progress.intersection_update(requests_to_retry)

    @property
    def name(self) -> str | None:
        return self._name

    @override
    async def get_handled_count(self) -> int:
        state = await self._get_state()
        return state.next_index - len(state.in_progress)

    @override
    async def get_total_count(self) -> int:
        state = await self._get_state()
        return state.next_index

    @override
    async def get_pending_count(self) -> int:
        # The source is read lazily, so like the total count, this leaves out the requests not fetched yet.
        state = await self._get_state()
        return len(state.in_progress)

    @override
    async def check_readiness(self) -> RequestLoaderStatus:
        state = await self._get_state()
        await self._ensure_next_request()
        if self._requests_to_retry or self._next[0] is not None:
            return ReadyStatus()

        return WaitingStatus() if state.in_progress else FinishedStatus()

    @override
    async def fetch_next_request(self) -> Request | None:
        state = await self._get_state()
        if self._requests_to_retry:
            return self._requests_to_retry.popleft()

        await self._ensure_next_request()

        if self._next[0] is None:
            return None

        state.in_progress.add(self._next[0].unique_key)

        next_request = self._next[0]
        if next_request is not None:
            state.next_index += 1
            state.next_unique_key = self._next[1].unique_key if self._next[1] is not None else None

        self._next = (self._next[1], None)
        await self._ensure_next_request()

        return next_request

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        state = await self._get_state()
        state.in_progress.remove(request.unique_key)

    async def _ensure_next_request(self) -> None:
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
        while count > 0:
            try:
                processing_request = await self._requests.__anext__()
                try:
                    request = self._transform_request(processing_request)
                except ValidationError:
                    logger.warning('Invalid request encountered in the request list: %s', processing_request)
                    continue
                yield request
            except StopAsyncIteration:
                yield None
            count -= 1

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
