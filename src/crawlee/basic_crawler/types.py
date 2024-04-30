# ruff: noqa: TCH003
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Coroutine, Protocol, Sequence

from typing_extensions import NotRequired, TypedDict, Unpack

if TYPE_CHECKING:
    from crawlee import Glob
    from crawlee.enqueue_strategy import EnqueueStrategy
    from crawlee.http_clients.base_http_client import HttpResponse
    from crawlee.request import BaseRequestData, Request


class AddRequestsFunctionKwargs(TypedDict):
    """Keyword arguments type for AddRequestsFunction."""

    limit: NotRequired[int]
    base_url: NotRequired[str]
    strategy: NotRequired[EnqueueStrategy]
    include: NotRequired[list[re.Pattern | Glob]]
    exclude: NotRequired[list[re.Pattern | Glob]]


class AddRequestsFunction(Protocol):
    """Type of a function for adding URLs to the request queue with optional filtering."""

    def __call__(  # noqa: D102
        self, requests: Sequence[str | BaseRequestData], **kwargs: Unpack[AddRequestsFunctionKwargs]
    ) -> Coroutine[None, None, None]: ...


class EnqueueLinksFunction(Protocol):
    """Type of a function for enqueueing links based on a selector."""

    def __call__(  # noqa: D102
        self,
        *,
        selector: str = 'a',
        label: str | None = None,
        user_data: dict[str, Any] | None = None,
        **kwargs: Unpack[AddRequestsFunctionKwargs],
    ) -> Coroutine[None, None, None]: ...


class SendRequestFunction(Protocol):
    """Type of a function for performing an HTTP request."""

    def __call__(  # noqa: D102
        self, url: str, *, method: str = 'get', headers: dict[str, str] | None = None
    ) -> Coroutine[None, None, HttpResponse]: ...


@dataclass(frozen=True)
class BasicCrawlingContext:
    """Basic crawling context intended to be extended by crawlers."""

    request: Request
    send_request: SendRequestFunction
    add_requests: AddRequestsFunction


@dataclass(frozen=True)
class FinalStatistics:
    """Statistics about a crawler run."""


class AddRequestsFunctionCall(AddRequestsFunctionKwargs):
    """Record of a call to `add_requests`."""

    requests: Sequence[str | BaseRequestData]


@dataclass()
class RequestHandlerRunResult:
    """Record of calls to storage-related context helpers."""

    add_requests_calls: list[AddRequestsFunctionCall] = field(default_factory=list)

    async def add_requests(
        self, requests: Sequence[str | BaseRequestData], **kwargs: Unpack[AddRequestsFunctionKwargs]
    ) -> None:
        """Track a call to the `add_requests` context helper."""
        self.add_requests_calls.append(AddRequestsFunctionCall(requests=requests, **kwargs))
