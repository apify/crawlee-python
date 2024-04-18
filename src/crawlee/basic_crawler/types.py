# ruff: noqa: TCH003
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Coroutine, Protocol

from typing_extensions import NotRequired, TypedDict, Unpack

if TYPE_CHECKING:
    from crawlee.globs import Glob
    from crawlee.http_clients.base_http_client import HttpResponse
    from crawlee.request import Request


class EnqueueStrategy(str, Enum):
    """Strategy for deciding which links should be followed and which ones should be ignored."""

    ALL = 'all'
    SAME_DOMAIN = 'same-domain'
    SAME_HOSTNAME = 'same-hostname'
    SAME_ORIGIN = 'same-origin'


class AddRequestsFunctionKwargs(TypedDict):
    """Keyword arguments type for AddRequestsFunction."""

    label: NotRequired[str]
    user_data: NotRequired[dict[str, Any]]
    limit: NotRequired[int]
    base_url: NotRequired[str]
    strategy: NotRequired[EnqueueStrategy]
    include: NotRequired[list[re.Pattern | Glob]]
    exclude: NotRequired[list[re.Pattern | Glob]]


class AddRequestsFunction(Protocol):
    """Type of a function for adding URLs to the request queue with optional filtering."""

    def __call__(  # noqa: D102
        self, urls: list[str], **kwargs: Unpack[AddRequestsFunctionKwargs]
    ) -> Coroutine[None, None, None]: ...


class EnqueueLinksFunction(Protocol):
    """Type of a function for enqueueing links based on a selector."""

    def __call__(  # noqa: D102
        self, *, selector: str, **kwargs: Unpack[AddRequestsFunctionKwargs]
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
