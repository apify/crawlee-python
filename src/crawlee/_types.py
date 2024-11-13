from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    Optional,
    Protocol,
    TypeVar,
    Union,
    cast,
    overload,
)

from pydantic import ConfigDict, Field, PlainValidator, RootModel
from typing_extensions import NotRequired, TypeAlias, TypedDict, Unpack

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    import logging
    import re
    from collections.abc import Coroutine, Sequence

    from crawlee import Glob
    from crawlee._request import BaseRequestData, Request
    from crawlee.base_storage_client._models import DatasetItemsListPage
    from crawlee.http_clients import HttpResponse
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions._session import Session
    from crawlee.storages._dataset import ExportToKwargs, GetDataKwargs
    from crawlee.storages._key_value_store import KeyValueStore

    # Workaround for https://github.com/pydantic/pydantic/issues/9445
    J = TypeVar('J', bound='JsonSerializable')
    JsonSerializable: TypeAlias = Union[
        list[J],
        dict[str, J],
        str,
        bool,
        int,
        float,
        None,
    ]
else:
    from pydantic import JsonValue as JsonSerializable


HttpMethod: TypeAlias = Literal['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

HttpPayload: TypeAlias = bytes


def _normalize_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Converts all header keys to lowercase, strips whitespace, and returns them sorted by key."""
    normalized_headers = {k.lower().strip(): v.strip() for k, v in headers.items()}
    sorted_headers = sorted(normalized_headers.items())
    return dict(sorted_headers)


class HttpHeaders(RootModel, Mapping[str, str]):
    """A dictionary-like object representing HTTP headers."""

    model_config = ConfigDict(populate_by_name=True)

    root: Annotated[
        dict[str, str],
        PlainValidator(lambda value: _normalize_headers(value)),
        Field(default_factory=dict),
    ] = {}  # noqa: RUF012

    def __getitem__(self, key: str) -> str:
        return self.root[key.lower()]

    def __setitem__(self, key: str, value: str) -> None:
        raise TypeError(f'{self.__class__.__name__} is immutable')

    def __delitem__(self, key: str) -> None:
        raise TypeError(f'{self.__class__.__name__} is immutable')

    def __or__(self, other: HttpHeaders) -> HttpHeaders:
        """Return a new instance of `HttpHeaders` combining this one with another one."""
        combined_headers = {**self.root, **other}
        return HttpHeaders(combined_headers)

    def __ror__(self, other: HttpHeaders) -> HttpHeaders:
        """Support reversed | operation (other | self)."""
        combined_headers = {**other, **self.root}
        return HttpHeaders(combined_headers)

    def __iter__(self) -> Iterator[str]:  # type: ignore
        yield from self.root

    def __len__(self) -> int:
        return len(self.root)


@docs_group('Data structures')
class EnqueueStrategy(str, Enum):
    """Strategy for deciding which links should be followed and which ones should be ignored."""

    ALL = 'all'
    """Enqueues all links found, regardless of the domain they point to. This strategy is useful when you
    want to follow every link, including those that navigate to external websites."""

    SAME_DOMAIN = 'same-domain'
    """Enqueues all links found that share the same domain name, including any possible subdomains.
    This strategy ensures that all links within the same top-level and base domain are included."""

    SAME_HOSTNAME = 'same-hostname'
    """Enqueues all links found for the exact same hostname. This is the default strategy, and it restricts
    the crawl to links that have the same hostname as the current page, excluding subdomains."""

    SAME_ORIGIN = 'same-origin'
    """Enqueues all links found that share the same origin. The same origin refers to URLs that share
    the same protocol, domain, and port, ensuring a strict scope for the crawl."""


@docs_group('Data structures')
class ConcurrencySettings:
    """Concurrency settings for AutoscaledPool."""

    def __init__(
        self,
        min_concurrency: int = 1,
        max_concurrency: int = 200,
        max_tasks_per_minute: float = float('inf'),
        desired_concurrency: int | None = None,
    ) -> None:
        """A default constructor.

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


class EnqueueLinksKwargs(TypedDict):
    """Keyword arguments for the `enqueue_links` methods."""

    limit: NotRequired[int]
    """Maximum number of requests to be enqueued."""

    base_url: NotRequired[str]
    """Base URL to be used for relative URLs."""

    strategy: NotRequired[EnqueueStrategy]
    """Enqueueing strategy, see the `EnqueueStrategy` enum for possible values and their meanings."""

    include: NotRequired[list[re.Pattern | Glob]]
    """List of regular expressions or globs that URLs must match to be enqueued."""

    exclude: NotRequired[list[re.Pattern | Glob]]
    """List of regular expressions or globs that URLs must not match to be enqueued."""


class AddRequestsKwargs(EnqueueLinksKwargs):
    """Keyword arguments for the `add_requests` methods."""

    requests: Sequence[str | BaseRequestData | Request]
    """Requests to be added to the request provider."""


class AddRequestsFunction(Protocol):
    """Type of a function for adding URLs to the request queue with optional filtering.

    This helper method simplifies the process of adding requests to the request provider.
    It opens the specified request provider and adds the requests to it.
    """

    def __call__(
        self,
        requests: Sequence[str | BaseRequestData | Request],
        **kwargs: Unpack[EnqueueLinksKwargs],
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


class PushDataKwargs(TypedDict):
    """Keyword arguments for dataset's `push_data` method."""


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


class PushDataFunctionCall(PushDataKwargs):
    data: JsonSerializable
    dataset_id: str | None
    dataset_name: str | None


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
    """A function type for enqueueing new URLs to crawl, based on elements selected by a CSS selector.

    This function is used to extract and enqueue new URLs from the current page for further crawling.
    """

    def __call__(
        self,
        *,
        selector: str = 'a',
        label: str | None = None,
        user_data: dict[str, Any] | None = None,
        **kwargs: Unpack[EnqueueLinksKwargs],
    ) -> Coroutine[None, None, None]:
        """A call dunder method.

        Args:
            selector: CSS selector used to find the elements containing the links.
            label: Label for the newly created `Request` objects, used for request routing.
            user_data: User data to be provided to the newly created `Request` objects.
            **kwargs: Additional arguments for the `add_requests` method.
        """


class SendRequestFunction(Protocol):
    """Type of a function for performing an HTTP request."""

    def __call__(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
    ) -> Coroutine[None, None, HttpResponse]: ...


T = TypeVar('T')


class KeyValueStoreInterface(Protocol):
    """The (limited) part of the `KeyValueStore` interface that should be accessible from a request handler."""

    @overload
    async def get_value(self, key: str) -> Any: ...

    @overload
    async def get_value(self, key: str, default_value: T) -> T: ...

    @overload
    async def get_value(self, key: str, default_value: T | None = None) -> T | None: ...

    async def get_value(self, key: str, default_value: T | None = None) -> T | None: ...

    async def set_value(
        self,
        key: str,
        value: Any,
        content_type: str | None = None,
    ) -> None: ...


class GetKeyValueStoreFromRequestHandlerFunction(Protocol):
    """Type of a function for accessing a key-value store from within a request handler."""

    def __call__(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Coroutine[None, None, KeyValueStoreInterface]: ...


@dataclass(frozen=True)
@docs_group('Data structures')
class BasicCrawlingContext:
    """Basic crawling context intended to be extended by crawlers."""

    request: Request
    session: Session | None
    proxy_info: ProxyInfo | None
    send_request: SendRequestFunction
    add_requests: AddRequestsFunction
    push_data: PushDataFunction
    get_key_value_store: GetKeyValueStoreFromRequestHandlerFunction
    log: logging.Logger


@dataclass()
class KeyValueStoreValue:
    content: Any
    content_type: str | None


class KeyValueStoreChangeRecords:
    def __init__(self, actual_key_value_store: KeyValueStore) -> None:
        self.updates = dict[str, KeyValueStoreValue]()
        self._actual_key_value_store = actual_key_value_store

    async def set_value(
        self,
        key: str,
        value: Any,
        content_type: str | None = None,
    ) -> None:
        self.updates[key] = KeyValueStoreValue(value, content_type)

    @overload
    async def get_value(self, key: str) -> Any: ...

    @overload
    async def get_value(self, key: str, default_value: T) -> T: ...

    @overload
    async def get_value(self, key: str, default_value: T | None = None) -> T | None: ...

    async def get_value(self, key: str, default_value: T | None = None) -> T | None:
        if key in self.updates:
            return cast(T, self.updates[key].content)

        return await self._actual_key_value_store.get_value(key, default_value)


class GetKeyValueStoreFunction(Protocol):
    """Type of a function for accessing the live implementation of a key-value store."""

    def __call__(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Coroutine[None, None, KeyValueStore]: ...


class RequestHandlerRunResult:
    """Record of calls to storage-related context helpers."""

    def __init__(self, *, key_value_store_getter: GetKeyValueStoreFunction) -> None:
        self._key_value_store_getter = key_value_store_getter
        self.add_requests_calls = list[AddRequestsKwargs]()
        self.push_data_calls = list[PushDataFunctionCall]()
        self.key_value_store_changes = dict[tuple[Optional[str], Optional[str]], KeyValueStoreChangeRecords]()

    async def add_requests(
        self,
        requests: Sequence[str | BaseRequestData],
        **kwargs: Unpack[EnqueueLinksKwargs],
    ) -> None:
        """Track a call to the `add_requests` context helper."""
        self.add_requests_calls.append(AddRequestsKwargs(requests=requests, **kwargs))

    async def push_data(
        self,
        data: JsonSerializable,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[PushDataKwargs],
    ) -> None:
        """Track a call to the `push_data` context helper."""
        from crawlee.storages._dataset import Dataset

        await Dataset.check_and_serialize(data)

        self.push_data_calls.append(
            PushDataFunctionCall(
                data=data,
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                **kwargs,
            )
        )

    async def get_key_value_store(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> KeyValueStoreInterface:
        if (id, name) not in self.key_value_store_changes:
            self.key_value_store_changes[id, name] = KeyValueStoreChangeRecords(
                await self._key_value_store_getter(id=id, name=name)
            )

        return self.key_value_store_changes[id, name]
