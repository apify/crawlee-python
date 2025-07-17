from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    Protocol,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

from pydantic import ConfigDict, Field, PlainValidator, RootModel

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    import json
    import logging
    import re
    from collections.abc import Callable, Coroutine, Sequence

    from typing_extensions import NotRequired, Required, Unpack

    from crawlee import Glob, Request
    from crawlee._request import RequestOptions
    from crawlee.configuration import Configuration
    from crawlee.http_clients import HttpResponse
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.storage_clients import StorageClient
    from crawlee.storages import KeyValueStore

    # Workaround for https://github.com/pydantic/pydantic/issues/9445
    J = TypeVar('J', bound='JsonSerializable')
    JsonSerializable = list[J] | dict[str, J] | str | bool | int | float | None
else:
    from pydantic import JsonValue as JsonSerializable

T = TypeVar('T')

HttpMethod = Literal['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

HttpPayload = bytes

RequestTransformAction = Literal['skip', 'unchanged']

EnqueueStrategy = Literal['all', 'same-domain', 'same-hostname', 'same-origin']
"""Enqueue strategy to be used for determining which links to extract and enqueue."""

SkippedReason = Literal['robots_txt']

LogLevel = Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


def _normalize_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Convert all header keys to lowercase, strips whitespace, and returns them sorted by key."""
    normalized_headers = {k.lower().strip(): v.strip() for k, v in headers.items()}
    sorted_headers = sorted(normalized_headers.items())
    return dict(sorted_headers)


@docs_group('Data structures')
class HttpHeaders(RootModel, Mapping[str, str]):
    """A dictionary-like object representing HTTP headers."""

    model_config = ConfigDict(populate_by_name=True)

    root: Annotated[
        dict[str, str],
        PlainValidator(lambda value: _normalize_headers(value)),
        Field(default_factory=dict),
    ] = {}

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

    def __iter__(self) -> Iterator[str]:  # type: ignore[override]
        yield from self.root

    def __len__(self) -> int:
        return len(self.root)


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
        """Initialize a new instance.

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


class EnqueueLinksKwargs(TypedDict):
    """Keyword arguments for the `enqueue_links` methods."""

    limit: NotRequired[int]
    """Maximum number of requests to be enqueued."""

    base_url: NotRequired[str]
    """Base URL to be used for relative URLs."""

    strategy: NotRequired[EnqueueStrategy]
    """Enqueue strategy to be used for determining which links to extract and enqueue.

    Options:
        all: Enqueue every link encountered, regardless of the target domain. Use this option to ensure that all
            links, including those leading to external websites, are followed.
        same-domain: Enqueue links that share the same domain name as the current page, including any subdomains.
            This strategy is ideal for crawling within the same top-level domain while still allowing for subdomain
            exploration.
        same-hostname: Enqueue links only if they match the exact hostname of the current page. This is the default
            behavior and restricts the crawl to the current hostname, excluding subdomains.
        same-origin: Enqueue links that share the same origin as the current page. The origin is defined by the
            combination of protocol, domain, and port, ensuring a strict scope for the crawl.
    """

    include: NotRequired[list[re.Pattern | Glob]]
    """List of regular expressions or globs that URLs must match to be enqueued."""

    exclude: NotRequired[list[re.Pattern | Glob]]
    """List of regular expressions or globs that URLs must not match to be enqueued."""


class AddRequestsKwargs(EnqueueLinksKwargs):
    """Keyword arguments for the `add_requests` methods."""

    requests: Sequence[str | Request]
    """Requests to be added to the `RequestManager`."""


class PushDataKwargs(TypedDict):
    """Keyword arguments for dataset's `push_data` method."""


class PushDataFunctionCall(PushDataKwargs):
    data: list[dict[str, Any]] | dict[str, Any]
    dataset_id: str | None
    dataset_name: str | None


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
            return cast('T', self.updates[key].content)

        return await self._actual_key_value_store.get_value(key, default_value)


class RequestHandlerRunResult:
    """Record of calls to storage-related context helpers."""

    def __init__(self, *, key_value_store_getter: GetKeyValueStoreFunction) -> None:
        self._key_value_store_getter = key_value_store_getter
        self.add_requests_calls = list[AddRequestsKwargs]()
        self.push_data_calls = list[PushDataFunctionCall]()
        self.key_value_store_changes = dict[tuple[str | None, str | None], KeyValueStoreChangeRecords]()

    async def add_requests(
        self,
        requests: Sequence[str | Request],
        **kwargs: Unpack[EnqueueLinksKwargs],
    ) -> None:
        """Track a call to the `add_requests` context helper."""
        self.add_requests_calls.append(AddRequestsKwargs(requests=requests, **kwargs))

    async def push_data(
        self,
        data: list[dict[str, Any]] | dict[str, Any],
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[PushDataKwargs],
    ) -> None:
        """Track a call to the `push_data` context helper."""
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


@docs_group('Functions')
class AddRequestsFunction(Protocol):
    """Function for adding requests to the `RequestManager`, with optional filtering.

    It simplifies the process of adding requests to the `RequestManager`. It automatically opens
    the specified one and adds the provided requests.
    """

    def __call__(
        self,
        requests: Sequence[str | Request],
        **kwargs: Unpack[EnqueueLinksKwargs],
    ) -> Coroutine[None, None, None]:
        """Call dunder method.

        Args:
            requests: Requests to be added to the `RequestManager`.
            **kwargs: Additional keyword arguments.
        """


@docs_group('Functions')
class EnqueueLinksFunction(Protocol):
    """A function for enqueueing new URLs to crawl based on elements selected by a given selector or explicit requests.

    It adds explicitly passed `requests` to the `RequestManager` or it extracts URLs from the current page and enqueues
    them for further crawling. It allows filtering through selectors and other options. You can also specify labels and
    user data to be associated with the newly created `Request` objects.

    It should not be called with `selector`, `label`, `user_data` or `transform_request_function` arguments together
    with `requests` argument.

    For even more control over the enqueued links you can use combination of `ExtractLinksFunction` and
    `AddRequestsFunction`.
    """

    @overload
    def __call__(
        self,
        *,
        selector: str | None = None,
        label: str | None = None,
        user_data: dict[str, Any] | None = None,
        transform_request_function: Callable[[RequestOptions], RequestOptions | RequestTransformAction] | None = None,
        **kwargs: Unpack[EnqueueLinksKwargs],
    ) -> Coroutine[None, None, None]: ...

    @overload
    def __call__(
        self, *, requests: Sequence[str | Request] | None = None, **kwargs: Unpack[EnqueueLinksKwargs]
    ) -> Coroutine[None, None, None]: ...

    def __call__(
        self,
        *,
        selector: str | None = None,
        label: str | None = None,
        user_data: dict[str, Any] | None = None,
        transform_request_function: Callable[[RequestOptions], RequestOptions | RequestTransformAction] | None = None,
        requests: Sequence[str | Request] | None = None,
        **kwargs: Unpack[EnqueueLinksKwargs],
    ) -> Coroutine[None, None, None]:
        """Call enqueue links function.

        Args:
            selector: A selector used to find the elements containing the links. The behaviour differs based
                on the crawler used:
                - `PlaywrightCrawler` supports CSS and XPath selectors.
                - `ParselCrawler` supports CSS selectors.
                - `BeautifulSoupCrawler` supports CSS selectors.
            label: Label for the newly created `Request` objects, used for request routing.
            user_data: User data to be provided to the newly created `Request` objects.
            transform_request_function: A function that takes `RequestOptions` and returns either:
                - Modified `RequestOptions` to update the request configuration,
                - `'skip'` to exclude the request from being enqueued,
                - `'unchanged'` to use the original request options without modification.
            requests: Requests to be added to the `RequestManager`.
            **kwargs: Additional keyword arguments.
        """


@docs_group('Functions')
class ExtractLinksFunction(Protocol):
    """A function for extracting URLs to crawl based on elements selected by a given selector.

    It extracts URLs from the current page and allows filtering through selectors and other options. You can also
    specify labels and user data to be associated with the newly created `Request` objects.
    """

    def __call__(
        self,
        *,
        selector: str = 'a',
        label: str | None = None,
        user_data: dict[str, Any] | None = None,
        transform_request_function: Callable[[RequestOptions], RequestOptions | RequestTransformAction] | None = None,
        **kwargs: Unpack[EnqueueLinksKwargs],
    ) -> Coroutine[None, None, list[Request]]:
        """Call extract links function.

        Args:
            selector: A selector used to find the elements containing the links. The behaviour differs based
                on the crawler used:
                - `PlaywrightCrawler` supports CSS and XPath selectors.
                - `ParselCrawler` supports CSS selectors.
                - `BeautifulSoupCrawler` supports CSS selectors.
            label: Label for the newly created `Request` objects, used for request routing.
            user_data: User data to be provided to the newly created `Request` objects.
            transform_request_function: A function that takes `RequestOptions` and returns either:
                - Modified `RequestOptions` to update the request configuration,
                - `'skip'` to exclude the request from being enqueued,
                - `'unchanged'` to use the original request options without modification.
            **kwargs: Additional keyword arguments.
        """


@docs_group('Functions')
class GetKeyValueStoreFunction(Protocol):
    """A function for accessing a `KeyValueStore`.

    It retrieves an instance of a `KeyValueStore` based on its ID or name.
    """

    def __call__(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Coroutine[None, None, KeyValueStore]:
        """Call dunder method.

        Args:
            id: The ID of the `KeyValueStore` to get.
            name: The name of the `KeyValueStore` to get.
        """


class GetKeyValueStoreFromRequestHandlerFunction(Protocol):
    """A function for accessing a `KeyValueStore`.

    It retrieves an instance of a `KeyValueStore` based on its ID or name.
    """

    def __call__(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> Coroutine[None, None, KeyValueStoreInterface]:
        """Call dunder method.

        Args:
            id: The ID of the `KeyValueStore` to get.
            name: The name of the `KeyValueStore` to get.
        """


@docs_group('Functions')
class PushDataFunction(Protocol):
    """A function for pushing data to a `Dataset`.

    It simplifies the process of adding data to a `Dataset`. It opens the specified one and pushes
    the provided data to it.
    """

    def __call__(
        self,
        data: list[dict[str, Any]] | dict[str, Any],
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        **kwargs: Unpack[PushDataKwargs],
    ) -> Coroutine[None, None, None]:
        """Call dunder method.

        Args:
            data: The data to push to the `Dataset`.
            dataset_id: The ID of the `Dataset` to push the data to.
            dataset_name: The name of the `Dataset` to push the data to.
            **kwargs: Additional keyword arguments.
        """


@docs_group('Functions')
class SendRequestFunction(Protocol):
    """A function for sending HTTP requests.

    It simplifies the process of sending HTTP requests. It is implemented by the crawling context and is used
    within request handlers to send additional HTTP requests to target URLs.
    """

    def __call__(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        payload: HttpPayload | None = None,
        headers: HttpHeaders | dict[str, str] | None = None,
    ) -> Coroutine[None, None, HttpResponse]:
        """Call send request function.

        Args:
            url: The URL to send the request to.
            method: The HTTP method to use.
            headers: The headers to include in the request.
            payload: The payload to include in the request.

        Returns:
            The HTTP response received from the server.
        """


@docs_group('Data structures')
@dataclasses.dataclass
class PageSnapshot:
    """Snapshot of a crawled page."""

    screenshot: bytes | None = None
    """Screenshot of the page format."""

    html: str | None = None
    """HTML content of the page."""

    def __bool__(self) -> bool:
        return bool(self.screenshot or self.html)


@docs_group('Functions')
class UseStateFunction(Protocol):
    """A function for managing state within the crawling context.

    It allows the use of persistent state across multiple crawls.

    Warning:
        This is an experimental feature. The behavior and interface may change in future versions.
    """

    def __call__(
        self,
        default_value: dict[str, JsonSerializable] | None = None,
    ) -> Coroutine[None, None, dict[str, JsonSerializable]]:
        """Call dunder method.

        Args:
            default_value: The default value to initialize the state if it is not already set.

        Returns:
            The current state.
        """


@dataclass(frozen=True)
@docs_group('Data structures')
class BasicCrawlingContext:
    """Basic crawling context.

    It represents the fundamental crawling context used by the `BasicCrawler`. It is extended by more
    specific crawlers to provide additional functionality.
    """

    request: Request
    """Request object for the current page being processed."""

    session: Session | None
    """Session object for the current page being processed."""

    proxy_info: ProxyInfo | None
    """Proxy information for the current page being processed."""

    send_request: SendRequestFunction
    """Send request crawling context helper function."""

    add_requests: AddRequestsFunction
    """Add requests crawling context helper function."""

    push_data: PushDataFunction
    """Push data crawling context helper function."""

    use_state: UseStateFunction
    """Use state crawling context helper function."""

    get_key_value_store: GetKeyValueStoreFromRequestHandlerFunction
    """Get key-value store crawling context helper function."""

    log: logging.Logger
    """Logger instance."""

    async def get_snapshot(self) -> PageSnapshot:
        """Get snapshot of crawled page."""
        return PageSnapshot()

    def __hash__(self) -> int:
        """Return hash of the context. Each context is considered unique."""
        return id(self)


class GetDataKwargs(TypedDict):
    """Keyword arguments for dataset's `get_data` method."""

    offset: NotRequired[int]
    """Skips the specified number of items at the start."""

    limit: NotRequired[int | None]
    """The maximum number of items to retrieve. Unlimited if None."""

    clean: NotRequired[bool]
    """Return only non-empty items and excludes hidden fields. Shortcut for `skip_hidden` and `skip_empty`."""

    desc: NotRequired[bool]
    """Set to True to sort results in descending order."""

    fields: NotRequired[list[str]]
    """Fields to include in each item. Sorts fields as specified if provided."""

    omit: NotRequired[list[str]]
    """Fields to exclude from each item."""

    unwind: NotRequired[str]
    """Unwinds items by a specified array field, turning each element into a separate item."""

    skip_empty: NotRequired[bool]
    """Excludes empty items from the results if True."""

    skip_hidden: NotRequired[bool]
    """Excludes fields starting with '#' if True."""

    flatten: NotRequired[list[str]]
    """Fields to be flattened in returned items."""

    view: NotRequired[str]
    """Specifies the dataset view to be used."""


class ExportToKwargs(TypedDict):
    """Keyword arguments for dataset's `export_to` method."""

    key: Required[str]
    """The key under which to save the data."""

    content_type: NotRequired[Literal['json', 'csv']]
    """The format in which to export the data. Either 'json' or 'csv'."""

    to_kvs_id: NotRequired[str]
    """ID of the key-value store to save the exported file."""

    to_kvs_name: NotRequired[str]
    """Name of the key-value store to save the exported file."""

    to_kvs_storage_client: NotRequired[StorageClient]
    """The storage client to use for saving the exported file."""

    to_kvs_configuration: NotRequired[Configuration]
    """The configuration to use for saving the exported file."""


class ExportDataJsonKwargs(TypedDict):
    """Keyword arguments for dataset's `export_data_json` method."""

    skipkeys: NotRequired[bool]
    """If True (default: False), dict keys that are not of a basic type (str, int, float, bool, None) will be skipped
    instead of raising a `TypeError`."""

    ensure_ascii: NotRequired[bool]
    """Determines if non-ASCII characters should be escaped in the output JSON string."""

    check_circular: NotRequired[bool]
    """If False (default: True), skips the circular reference check for container types. A circular reference will
    result in a `RecursionError` or worse if unchecked."""

    allow_nan: NotRequired[bool]
    """If False (default: True), raises a ValueError for out-of-range float values (nan, inf, -inf) to strictly comply
    with the JSON specification. If True, uses their JavaScript equivalents (NaN, Infinity, -Infinity)."""

    cls: NotRequired[type[json.JSONEncoder]]
    """Allows specifying a custom JSON encoder."""

    indent: NotRequired[int]
    """Specifies the number of spaces for indentation in the pretty-printed JSON output."""

    separators: NotRequired[tuple[str, str]]
    """A tuple of (item_separator, key_separator). The default is (', ', ': ') if indent is None and (',', ': ')
    otherwise."""

    default: NotRequired[Callable]
    """A function called for objects that can't be serialized otherwise. It should return a JSON-encodable version
    of the object or raise a `TypeError`."""

    sort_keys: NotRequired[bool]
    """Specifies whether the output JSON object should have keys sorted alphabetically."""


class ExportDataCsvKwargs(TypedDict):
    """Keyword arguments for dataset's `export_data_csv` method."""

    dialect: NotRequired[str]
    """Specifies a dialect to be used in CSV parsing and writing."""

    delimiter: NotRequired[str]
    """A one-character string used to separate fields. Defaults to ','."""

    doublequote: NotRequired[bool]
    """Controls how instances of `quotechar` inside a field should be quoted. When True, the character is doubled;
    when False, the `escapechar` is used as a prefix. Defaults to True."""

    escapechar: NotRequired[str]
    """A one-character string used to escape the delimiter if `quoting` is set to `QUOTE_NONE` and the `quotechar`
    if `doublequote` is False. Defaults to None, disabling escaping."""

    lineterminator: NotRequired[str]
    """The string used to terminate lines produced by the writer. Defaults to '\\r\\n'."""

    quotechar: NotRequired[str]
    """A one-character string used to quote fields containing special characters, like the delimiter or quotechar,
    or fields containing new-line characters. Defaults to '\"'."""

    quoting: NotRequired[int]
    """Controls when quotes should be generated by the writer and recognized by the reader. Can take any of
    the `QUOTE_*` constants, with a default of `QUOTE_MINIMAL`."""

    skipinitialspace: NotRequired[bool]
    """When True, spaces immediately following the delimiter are ignored. Defaults to False."""

    strict: NotRequired[bool]
    """When True, raises an exception on bad CSV input. Defaults to False."""
