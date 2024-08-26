from __future__ import annotations

from collections.abc import Coroutine, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Protocol, Union

from typing_extensions import NotRequired, TypeAlias, TypedDict, Unpack

if TYPE_CHECKING:
    import logging
    import re

    from crawlee import Glob
    from crawlee._request import BaseRequestData, Request
    from crawlee.base_storage_client._models import DatasetItemsListPage
    from crawlee.http_clients import HttpResponse
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions._session import Session
    from crawlee.storages._dataset import ExportToKwargs, GetDataKwargs, PushDataKwargs

# Type for representing json-serializable values. It's close enough to the real thing supported
# by json.parse, and the best we can do until mypy supports recursive types. It was suggested
# in a discussion with (and approved by) Guido van Rossum, so I'd consider it correct enough.
JsonSerializable: TypeAlias = Union[str, int, float, bool, None, dict[str, Any], list[Any]]

HttpMethod: TypeAlias = Literal['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']


class EnqueueStrategy(str, Enum):
    """Strategy for deciding which links should be followed and which ones should be ignored."""

    ALL = 'all'
    SAME_DOMAIN = 'same-domain'
    SAME_HOSTNAME = 'same-hostname'
    SAME_ORIGIN = 'same-origin'


class ConcurrencySettings:
    """Concurrency settings for AutoscaledPool."""

    def __init__(
        self,
        min_concurrency: int = 1,
        max_concurrency: int = 200,
        max_tasks_per_minute: float = float('inf'),
        desired_concurrency: int | None = None,
    ) -> None:
        """Creates a new instance.

        Args:
            min_concurrency: The minimum number of tasks running in parallel. If you set this value too high
                with respect to the available system memory and CPU, your code might run extremely slow or crash.
            max_concurrency: The maximum number of tasks running in parallel.
            max_tasks_per_minute: The maximum number of tasks per minute the pool can run. By default, this is set
                to infinity, but you can pass any positive, non-zero number.
            desired_concurrency: The desired number of tasks that should be running parallel on the start of the pool,
                if there is a large enough supply of them. By default, it is `min_concurrency`.
        """
        if desired_concurrency is not None and desired_concurrency < 1:
            raise ValueError('desired_concurrency must be 1 or larger')

        if min_concurrency < 1:
            raise ValueError('min_concurrency must be 1 or larger')

        if max_concurrency < min_concurrency:
            raise ValueError('max_concurrency cannot be less than min_concurrency')

        if max_tasks_per_minute <= 0:
            raise ValueError('max_tasks_per_minute must be positive')

        self.min_concurrency = min_concurrency
        self.max_concurrency = max_concurrency
        self.desired_concurrency = desired_concurrency if desired_concurrency is not None else min_concurrency
        self.max_tasks_per_minute = max_tasks_per_minute


class StorageTypes(str, Enum):
    """Possible Crawlee storage types."""

    DATASET = 'Dataset'
    KEY_VALUE_STORE = 'Key-value store'
    REQUEST_QUEUE = 'Request queue'


class AddRequestsKwargs(TypedDict):
    """Keyword arguments for crawler's `add_requests` method."""

    limit: NotRequired[int]
    base_url: NotRequired[str]
    strategy: NotRequired[EnqueueStrategy]
    include: NotRequired[list[re.Pattern | Glob]]
    exclude: NotRequired[list[re.Pattern | Glob]]


class AddRequestsFunctionCall(AddRequestsKwargs):
    """Record of a call to `add_requests`."""

    requests: Sequence[str | BaseRequestData | Request]


class AddRequestsFunction(Protocol):
    """Type of a function for adding URLs to the request queue with optional filtering.

    This helper method simplifies the process of adding requests to the request provider. It opens the specified
    request provider and adds the requests to it.
    """

    def __call__(
        self,
        requests: Sequence[str | BaseRequestData | Request],
        **kwargs: Unpack[AddRequestsKwargs],
    ) -> Coroutine[None, None, None]: ...


class GetDataFunction(Protocol):
    """Type of a function for getting data from the dataset.

    This helper method simplifies the process of retrieving data from a dataset. It opens the specified
    dataset and then retrieves the data based on the provided parameters.
    """

    def __call__(
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

    def __call__(
        self,
        data: JsonSerializable,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[PushDataKwargs],
    ) -> Coroutine[None, None, None]: ...


class ExportToFunction(Protocol):
    """Type of a function for exporting data from a dataset.

    This helper method simplifies the process of exporting data from a dataset. It opens the specified
    dataset and then exports its content to the key-value store.
    """

    def __call__(
        self,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[ExportToKwargs],
    ) -> Coroutine[None, None, None]: ...


class EnqueueLinksFunction(Protocol):
    """Type of a function for enqueueing links based on a selector.

    Args:
        selector: CSS selector used to find the elements containing the links.
        label: Label for the newly created `Request` objects, used for request routing.
        user_data: User data to be provided to the newly created `Request` objects.
        **kwargs: Additional arguments for the `add_requests` method.
    """

    def __call__(
        self,
        *,
        selector: str = 'a',
        label: str | None = None,
        user_data: dict[str, Any] | None = None,
        **kwargs: Unpack[AddRequestsKwargs],
    ) -> Coroutine[None, None, None]: ...


class SendRequestFunction(Protocol):
    """Type of a function for performing an HTTP request."""

    def __call__(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | None = None,
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


class HttpHeaders(Mapping[str, str]):
    """An immutable mapping for HTTP headers that ensures case-insensitivity for header names."""

    def __init__(self, headers: Mapping[str, str] | None = None) -> None:
        """Create a new instance.

        Args:
            headers: A mapping of header names to values.
        """
        # Ensure immutability by sorting and fixing the order.
        headers = headers or {}
        headers = {k.lower(): v for k, v in headers.items()}
        self._headers = dict(sorted(headers.items()))

    def __getitem__(self, key: str) -> str:
        """Get the value of a header by its name, case-insensitive."""
        return self._headers[key.lower()]

    def __iter__(self) -> Iterator[str]:
        """Return an iterator over the header names."""
        return iter(self._headers)

    def __len__(self) -> int:
        """Return the number of headers."""
        return len(self._headers)

    def __repr__(self) -> str:
        """Return a string representation of the object."""
        return f'{self.__class__.__name__}({self._headers})'

    def __setitem__(self, key: str, value: str) -> None:
        """Prevent setting a header, as the object is immutable."""
        raise TypeError(f'{self.__class__.__name__} is immutable')

    def __delitem__(self, key: str) -> None:
        """Prevent deleting a header, as the object is immutable."""
        raise TypeError(f'{self.__class__.__name__} is immutable')
