# ruff: noqa: TCH003
from __future__ import annotations

import logging
import re
from collections.abc import Coroutine, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from typing_extensions import NotRequired, TypedDict, Unpack

if TYPE_CHECKING:
    from crawlee import Glob
    from crawlee.enqueue_strategy import EnqueueStrategy
    from crawlee.http_clients.base_http_client import HttpResponse
    from crawlee.models import BaseRequestData, DatasetItemsListPage, Request
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions.session import Session
    from crawlee.storages.dataset import ExportToKwargs, GetDataKwargs, PushDataKwargs
    from crawlee.types import JSONSerializable


class AddRequestsKwargs(TypedDict):
    """Keyword arguments for crawler's `add_requests` method."""

    limit: NotRequired[int]
    base_url: NotRequired[str]
    strategy: NotRequired[EnqueueStrategy]
    include: NotRequired[list[re.Pattern | Glob]]
    exclude: NotRequired[list[re.Pattern | Glob]]


class AddRequestsFunction(Protocol):
    """Type of a function for adding URLs to the request queue with optional filtering.

    This helper method simplifies the process of adding requests to the request provider. It opens the specified
    request provider and adds the requests to it.
    """

    def __call__(  # noqa: D102
        self,
        requests: Sequence[str | BaseRequestData | Request],
        **kwargs: Unpack[AddRequestsKwargs],
    ) -> Coroutine[None, None, None]: ...


class GetDataFunction(Protocol):
    """Type of a function for getting data from the dataset.

    This helper method simplifies the process of retrieving data from a dataset. It opens the specified
    dataset and then retrieves the data based on the provided parameters.
    """

    def __call__(  # noqa: D102
        self,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[GetDataKwargs],
    ) -> Coroutine[None, None, DatasetItemsListPage]: ...


class PushDataFunction(Protocol):
    """Type of a function for pushing data to the dataset.

    This helper method simplifies the process of pushing data to a dataset. It opens the specified
    dataset and then pushes the provided data to it.
    """

    def __call__(  # noqa: D102
        self,
        data: JSONSerializable,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[PushDataKwargs],
    ) -> Coroutine[None, None, None]: ...


class ExportToFunction(Protocol):
    """Type of a function for exporting data from a dataset.

    This helper method simplifies the process of exporting data from a dataset. It opens the specified
    dataset and then exports its content to the key-value store.
    """

    def __call__(  # noqa: D102
        self,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[ExportToKwargs],
    ) -> Coroutine[None, None, None]: ...


class EnqueueLinksFunction(Protocol):
    """Type of a function for enqueueing links based on a selector."""

    def __call__(  # noqa: D102
        self,
        *,
        selector: str = 'a',
        label: str | None = None,
        user_data: dict[str, Any] | None = None,
        **kwargs: Unpack[AddRequestsKwargs],
    ) -> Coroutine[None, None, None]: ...


class SendRequestFunction(Protocol):
    """Type of a function for performing an HTTP request."""

    def __call__(  # noqa: D102
        self,
        url: str,
        *,
        method: str = 'get',
        headers: dict[str, str] | None = None,
    ) -> Coroutine[None, None, HttpResponse]: ...


@dataclass(frozen=True)
class BasicCrawlingContext:
    """Basic crawling context intended to be extended by crawlers."""

    request: Request
    session: Session | None
    proxy_info: ProxyInfo | None
    send_request: SendRequestFunction
    add_requests: AddRequestsFunction
    push_data: PushDataFunction
    log: logging.Logger


class AddRequestsFunctionCall(AddRequestsKwargs):
    """Record of a call to `add_requests`."""

    requests: Sequence[str | BaseRequestData | Request]


@dataclass()
class RequestHandlerRunResult:
    """Record of calls to storage-related context helpers."""

    add_requests_calls: list[AddRequestsFunctionCall] = field(default_factory=list)

    async def add_requests(
        self,
        requests: Sequence[str | BaseRequestData],
        **kwargs: Unpack[AddRequestsKwargs],
    ) -> None:
        """Track a call to the `add_requests` context helper."""
        self.add_requests_calls.append(AddRequestsFunctionCall(requests=requests, **kwargs))
