# ruff: noqa: TCH001, TCH002, TCH003 (because of Pydantic)

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from datetime import datetime
from decimal import Decimal
from enum import IntEnum
from typing import Annotated, Any, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    PlainSerializer,
    PlainValidator,
    TypeAdapter,
)
from typing_extensions import Self

from crawlee._types import EnqueueStrategy, HttpMethod, HttpPayload, HttpQueryParams
from crawlee._utils.requests import compute_unique_key, unique_key_to_request_id
from crawlee._utils.urls import extract_query_params, validate_http_url


def _normalize_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Converts all header keys to lowercase and returns them sorted by key."""
    normalized_headers = {k.lower(): v for k, v in headers.items()}
    sorted_headers = sorted(normalized_headers.items())
    return dict(sorted_headers)


class HttpHeaders(BaseModel, Mapping[str, str]):
    """A dictionary-like object representing HTTP headers."""

    model_config = ConfigDict(populate_by_name=True)

    headers: Annotated[
        dict[str, str],
        PlainValidator(lambda value: _normalize_headers(value)),
        Field(default_factory=dict),
    ] = {}

    def __getitem__(self, key: str) -> str:
        return self.headers[key.lower()]

    def __setitem__(self, key: str, value: str) -> None:
        # self.headers[key] = value
        raise TypeError(f'{self.__class__.__name__} is immutable')

    def __delitem__(self, key: str) -> None:
        # del self.headers[key]
        raise TypeError(f'{self.__class__.__name__} is immutable')

    def __or__(self, other: HttpHeaders) -> HttpHeaders:
        """Return a new instance of `HttpHeaders` combining this one with another one."""
        combined_headers = {**self.headers, **other}
        return HttpHeaders(headers=combined_headers)

    def __ror__(self, other: HttpHeaders) -> HttpHeaders:
        """Support reversed | operation (other | self)."""
        combined_headers = {**other, **self.headers}
        return HttpHeaders(headers=combined_headers)

    def __iter__(self) -> Iterator[str]:  # type: ignore
        yield from self.headers

    def __len__(self) -> int:
        return len(self.headers)

    def to_dict(self) -> dict[str, str]:
        return dict(self.model_dump().get('headers', {}))


# http_headers_adapter = TypeAdapter(HttpHeaders)


class RequestState(IntEnum):
    """Crawlee-specific request handling state."""

    UNPROCESSED = 0
    BEFORE_NAV = 1
    AFTER_NAV = 2
    REQUEST_HANDLER = 3
    DONE = 4
    ERROR_HANDLER = 5
    ERROR = 6
    SKIPPED = 7


class CrawleeRequestData(BaseModel):
    """Crawlee-specific configuration stored in the `user_data`."""

    max_retries: Annotated[int | None, Field(alias='maxRetries')] = None
    """Maximum number of retries for this request. Allows to override the global `max_request_retries` option of
    `BasicCrawler`."""

    enqueue_strategy: Annotated[str | None, Field(alias='enqueueStrategy')] = None

    state: RequestState | None = None
    """Describes the request's current lifecycle state."""

    session_rotation_count: Annotated[int | None, Field(alias='sessionRotationCount')] = None

    skip_navigation: Annotated[bool, Field(alias='skipNavigation')] = False

    last_proxy_tier: Annotated[int | None, Field(alias='lastProxyTier')] = None

    forefront: Annotated[bool, Field()] = False


class UserData(BaseModel, MutableMapping[str, JsonValue]):
    """Represents the `user_data` part of a Request.

    Apart from the well-known attributes (`label` and `__crawlee`), it can also contain arbitrary JSON-compatible
    values.
    """

    model_config = ConfigDict(extra='allow')
    __pydantic_extra__: dict[str, JsonValue] = Field(init=False)  # pyright: ignore

    crawlee_data: Annotated[CrawleeRequestData | None, Field(alias='__crawlee')] = None
    label: Annotated[str | None, Field()] = None

    def __getitem__(self, key: str) -> JsonValue:
        return self.__pydantic_extra__[key]

    def __setitem__(self, key: str, value: JsonValue) -> None:
        if key == 'label':
            if value is not None and not isinstance(value, str):
                raise ValueError('`label` must be str or None')

            self.label = value
        self.__pydantic_extra__[key] = value

    def __delitem__(self, key: str) -> None:
        del self.__pydantic_extra__[key]

    def __iter__(self) -> Iterator[str]:  # type: ignore
        yield from self.__pydantic_extra__

    def __len__(self) -> int:
        return len(self.__pydantic_extra__)


user_data_adapter = TypeAdapter(UserData)


class BaseRequestData(BaseModel):
    """Data needed to create a new crawling request."""

    model_config = ConfigDict(populate_by_name=True)

    url: Annotated[str, BeforeValidator(validate_http_url), Field()]
    """URL of the web page to crawl"""

    unique_key: Annotated[str, Field(alias='uniqueKey')]
    """A unique key identifying the request. Two requests with the same `unique_key` are considered as pointing
    to the same URL.

    If `unique_key` is not provided, then it is automatically generated by normalizing the URL.
    For example, the URL of `HTTP://www.EXAMPLE.com/something/` will produce the `unique_key`
    of `http://www.example.com/something`.

    Pass an arbitrary non-empty text value to the `unique_key` property
    to override the default behavior and specify which URLs shall be considered equal.
    """

    method: HttpMethod = 'GET'
    """HTTP request method."""

    headers: Annotated[HttpHeaders, Field(default_factory=HttpHeaders())] = HttpHeaders()
    """HTTP request headers."""

    query_params: Annotated[HttpQueryParams, Field(alias='queryParams', default_factory=dict)] = {}
    """URL query parameters."""

    payload: HttpPayload | None = None

    data: Annotated[dict[str, Any], Field(default_factory=dict)] = {}

    user_data: Annotated[
        dict[str, JsonValue],  # Internally, the model contains `UserData`, this is just for convenience
        Field(alias='userData', default_factory=lambda: UserData()),
        PlainValidator(user_data_adapter.validate_python),
        PlainSerializer(
            lambda instance: user_data_adapter.dump_python(
                instance,
                by_alias=True,
                exclude_none=True,
                exclude_unset=True,
                exclude_defaults=True,
            )
        ),
    ] = {}
    """Custom user data assigned to the request. Use this to save any request related data to the
    request's scope, keeping them accessible on retries, failures etc.
    """

    retry_count: Annotated[int, Field(alias='retryCount')] = 0

    no_retry: Annotated[bool, Field(alias='noRetry')] = False

    loaded_url: Annotated[str | None, BeforeValidator(validate_http_url), Field(alias='loadedUrl')] = None

    handled_at: Annotated[datetime | None, Field(alias='handledAt')] = None

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        method: HttpMethod = 'GET',
        payload: HttpPayload | None = None,
        label: str | None = None,
        unique_key: str | None = None,
        id: str | None = None,
        keep_url_fragment: bool = False,
        use_extended_unique_key: bool = False,
        **kwargs: Any,
    ) -> Self:
        """Create a new `BaseRequestData` instance from a URL. See `Request.from_url` for more details."""
        unique_key = unique_key or compute_unique_key(
            url,
            method=method,
            payload=payload,
            keep_url_fragment=keep_url_fragment,
            use_extended_unique_key=use_extended_unique_key,
        )

        id = id or unique_key_to_request_id(unique_key)

        request = cls(
            url=url,
            unique_key=unique_key,
            id=id,
            method=method,
            payload=payload,
            **kwargs,
        )

        if label is not None:
            request.user_data['label'] = label

        return request

    def get_query_param_from_url(self, param: str, *, default: str | None = None) -> str | None:
        """Get the value of a specific query parameter from the URL."""
        query_params = extract_query_params(self.url)
        values = query_params.get(param, [default])  # parse_qs returns values as list
        return values[0]


class Request(BaseRequestData):
    """Represents a request in the Crawlee framework, containing the necessary information for crawling operations.

    The `Request` class is one of the core components in Crawlee, utilized by various components such as request
    providers, HTTP clients, crawlers, and more. It encapsulates the essential data for executing web requests,
    including the URL, HTTP method, headers, payload, and user data. The user data allows custom information
    to be stored and persisted throughout the request lifecycle, including its retries.

    Key functionalities include managing the request's identifier (`id`), unique key (`unique_key`) that is used
    for request deduplication, controlling retries, handling state management, and enabling configuration for session
    rotation and proxy handling.

    The recommended way to create a new instance is by using the `Request.from_url` constructor, which automatically
    generates a unique key and identifier based on the URL and request parameters.

    ```python
    request = Request.from_url('https://crawlee.dev')
    ```
    """

    id: str

    json_: str | None = None  # TODO: get rid of this
    # https://github.com/apify/crawlee-python/issues/94

    order_no: Decimal | None = None  # TODO: get rid of this
    # https://github.com/apify/crawlee-python/issues/94

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        method: HttpMethod = 'GET',
        payload: HttpPayload | None = None,
        label: str | None = None,
        unique_key: str | None = None,
        id: str | None = None,
        keep_url_fragment: bool = False,
        use_extended_unique_key: bool = False,
        **kwargs: Any,
    ) -> Self:
        """Create a new `Request` instance from a URL.

        This is recommended constructor for creating new `Request` instances. It generates a `Request` object from
        a given URL with additional options to customize HTTP method, payload, unique key, and other request
        properties. If no `unique_key` or `id` is provided, they are computed automatically based on the URL,
        method and payload. It depends on the `keep_url_fragment` and `use_extended_unique_key` flags.

        Args:
            url: The URL of the request.
            method: The HTTP method of the request.
            payload: The data to be sent as the request body. Typically used with 'POST' or 'PUT' requests.
            label: A custom label to differentiate between request types. This is stored in `user_data`, and it is
                used for request routing (different requests go to different handlers).
            unique_key: A unique key identifying the request. If not provided, it is automatically computed based on
                the URL and other parameters. Requests with the same `unique_key` are treated as identical.
            id: A unique identifier for the request. If not provided, it is automatically generated from the
                `unique_key`.
            keep_url_fragment: Determines whether the URL fragment (e.g., `#section`) should be included in
                the `unique_key` computation. This is only relevant when `unique_key` is not provided.
            use_extended_unique_key: Determines whether to include the HTTP method and payload in the `unique_key`
                computation. This is only relevant when `unique_key` is not provided.
            **kwargs: Additional request properties.
        """
        unique_key = unique_key or compute_unique_key(
            url,
            method=method,
            payload=payload,
            keep_url_fragment=keep_url_fragment,
            use_extended_unique_key=use_extended_unique_key,
        )

        id = id or unique_key_to_request_id(unique_key)

        request = cls(
            url=url,
            unique_key=unique_key,
            id=id,
            method=method,
            payload=payload,
            **kwargs,
        )

        if label is not None:
            request.user_data['label'] = label

        return request

    @classmethod
    def from_base_request_data(cls, base_request_data: BaseRequestData, *, id: str | None = None) -> Self:
        """Create a complete Request object based on a BaseRequestData instance."""
        kwargs = base_request_data.model_dump()
        kwargs['id'] = id or unique_key_to_request_id(base_request_data.unique_key)
        return cls(**kwargs)

    @property
    def label(self) -> str | None:
        """A string used to differentiate between arbitrary request types."""
        return cast(UserData, self.user_data).label

    @property
    def crawlee_data(self) -> CrawleeRequestData:
        """Crawlee-specific configuration stored in the user_data."""
        user_data = cast(UserData, self.user_data)
        if user_data.crawlee_data is None:
            user_data.crawlee_data = CrawleeRequestData()

        return user_data.crawlee_data

    @property
    def state(self) -> RequestState | None:
        """Crawlee-specific request handling state."""
        return self.crawlee_data.state

    @state.setter
    def state(self, new_state: RequestState) -> None:
        self.crawlee_data.state = new_state

    @property
    def max_retries(self) -> int | None:
        """Crawlee-specific limit on the number of retries of the request."""
        return self.crawlee_data.max_retries

    @max_retries.setter
    def max_retries(self, new_max_retries: int) -> None:
        self.crawlee_data.max_retries = new_max_retries

    @property
    def session_rotation_count(self) -> int | None:
        """Crawlee-specific number of finished session rotations for the request."""
        return self.crawlee_data.session_rotation_count

    @session_rotation_count.setter
    def session_rotation_count(self, new_session_rotation_count: int) -> None:
        self.crawlee_data.session_rotation_count = new_session_rotation_count

    @property
    def enqueue_strategy(self) -> EnqueueStrategy:
        """The strategy used when enqueueing the request."""
        return (
            EnqueueStrategy(self.crawlee_data.enqueue_strategy)
            if self.crawlee_data.enqueue_strategy
            else EnqueueStrategy.ALL
        )

    @enqueue_strategy.setter
    def enqueue_strategy(self, new_enqueue_strategy: EnqueueStrategy) -> None:
        self.crawlee_data.enqueue_strategy = new_enqueue_strategy

    @property
    def last_proxy_tier(self) -> int | None:
        """The last proxy tier used to process the request."""
        return self.crawlee_data.last_proxy_tier

    @last_proxy_tier.setter
    def last_proxy_tier(self, new_value: int) -> None:
        self.crawlee_data.last_proxy_tier = new_value

    @property
    def forefront(self) -> bool:
        """Should the request be enqueued at the start of the queue?"""
        return self.crawlee_data.forefront

    @forefront.setter
    def forefront(self, new_value: bool) -> None:
        self.crawlee_data.forefront = new_value


class RequestWithLock(Request):
    """A crawling request with information about locks."""

    lock_expires_at: Annotated[datetime, Field(alias='lockExpiresAt')]
