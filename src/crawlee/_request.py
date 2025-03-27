from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from datetime import datetime
from enum import IntEnum
from typing import TYPE_CHECKING, Annotated, Any, TypedDict, cast

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, PlainValidator, TypeAdapter
from yarl import URL

from crawlee._types import EnqueueStrategy, HttpHeaders, HttpMethod, HttpPayload, JsonSerializable
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee._utils.requests import compute_unique_key, unique_key_to_request_id
from crawlee._utils.urls import validate_http_url

if TYPE_CHECKING:
    from typing_extensions import NotRequired, Required, Self


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

    enqueue_strategy: Annotated[EnqueueStrategy | None, Field(alias='enqueueStrategy')] = None
    """The strategy that was used for enqueuing the request."""

    state: RequestState | None = None
    """Describes the request's current lifecycle state."""

    session_rotation_count: Annotated[int | None, Field(alias='sessionRotationCount')] = None
    """The number of finished session rotations for this request."""

    skip_navigation: Annotated[bool, Field(alias='skipNavigation')] = False

    last_proxy_tier: Annotated[int | None, Field(alias='lastProxyTier')] = None
    """The last proxy tier used to process the request."""

    forefront: Annotated[bool, Field()] = False
    """Indicate whether the request should be enqueued at the front of the queue."""

    crawl_depth: Annotated[int, Field(alias='crawlDepth')] = 0
    """The depth of the request in the crawl tree."""

    session_id: Annotated[str | None, Field()] = None
    """ID of a session to which the request is bound."""


class UserData(BaseModel, MutableMapping[str, JsonSerializable]):
    """Represents the `user_data` part of a Request.

    Apart from the well-known attributes (`label` and `__crawlee`), it can also contain arbitrary JSON-compatible
    values.
    """

    model_config = ConfigDict(extra='allow')
    __pydantic_extra__: dict[str, JsonSerializable] = Field(init=False)

    crawlee_data: Annotated[CrawleeRequestData | None, Field(alias='__crawlee')] = None
    """Crawlee-specific configuration stored in the `user_data`."""

    label: Annotated[str | None, Field()] = None
    """Label used for request routing."""

    def __getitem__(self, key: str) -> JsonSerializable:
        return self.__pydantic_extra__[key]

    def __setitem__(self, key: str, value: JsonSerializable) -> None:
        if key == 'label':
            if value is not None and not isinstance(value, str):
                raise ValueError('`label` must be str or None')

            self.label = value

        self.__pydantic_extra__[key] = value

    def __delitem__(self, key: str) -> None:
        del self.__pydantic_extra__[key]

    def __iter__(self) -> Iterator[str]:  # type: ignore[override]
        yield from self.__pydantic_extra__

    def __len__(self) -> int:
        return len(self.__pydantic_extra__)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BaseModel):
            return super().__eq__(other)

        if isinstance(other, dict):
            return self.model_dump() == other

        return NotImplemented


user_data_adapter = TypeAdapter(UserData)


class RequestOptions(TypedDict):
    """Options that can be used to customize request creation.

    This type exactly matches the parameters of `Request.from_url` method.
    """

    url: Required[str]
    method: NotRequired[HttpMethod]
    headers: NotRequired[HttpHeaders | dict[str, str] | None]
    payload: NotRequired[HttpPayload | str | None]
    label: NotRequired[str | None]
    session_id: NotRequired[str | None]
    unique_key: NotRequired[str | None]
    id: NotRequired[str | None]
    keep_url_fragment: NotRequired[bool]
    use_extended_unique_key: NotRequired[bool]
    always_enqueue: NotRequired[bool]
    user_data: NotRequired[dict[str, JsonSerializable]]
    no_retry: NotRequired[bool]


@docs_group('Data structures')
class Request(BaseModel):
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

    ### Usage

    ```python
    from crawlee import Request

    request = Request.from_url('https://crawlee.dev')
    ```
    """

    model_config = ConfigDict(populate_by_name=True)

    url: Annotated[str, BeforeValidator(validate_http_url), Field()]
    """The URL of the web page to crawl. Must be a valid HTTP or HTTPS URL, and may include query parameters
    and fragments."""

    method: HttpMethod = 'GET'
    """HTTP request method."""

    headers: Annotated[HttpHeaders, Field(default_factory=HttpHeaders)] = HttpHeaders()
    """HTTP request headers."""

    payload: Annotated[
        HttpPayload | None,
        BeforeValidator(lambda v: v.encode() if isinstance(v, str) else v),
        PlainSerializer(lambda v: v.decode() if isinstance(v, bytes) else v),
    ] = None
    """HTTP request payload."""

    user_data: Annotated[
        dict[str, JsonSerializable],  # Internally, the model contains `UserData`, this is just for convenience
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
    """Number of times the request has been retried."""

    no_retry: Annotated[bool, Field(alias='noRetry')] = False
    """If set to `True`, the request will not be retried in case of failure."""

    loaded_url: Annotated[str | None, BeforeValidator(validate_http_url), Field(alias='loadedUrl')] = None
    """URL of the web page that was loaded. This can differ from the original URL in case of redirects."""

    handled_at: Annotated[datetime | None, Field(alias='handledAt')] = None
    """Timestamp when the request was handled."""

    unique_key: Annotated[str, Field(alias='uniqueKey')]
    """A unique key identifying the request. Two requests with the same `unique_key` are considered as pointing
    to the same URL.

    If `unique_key` is not provided, then it is automatically generated by normalizing the URL.
    For example, the URL of `HTTP://www.EXAMPLE.com/something/` will produce the `unique_key`
    of `http://www.example.com/something`.

    Pass an arbitrary non-empty text value to the `unique_key` property
    to override the default behavior and specify which URLs shall be considered equal.
    """

    id: str
    """A unique identifier for the request. Note that this is not used for deduplication, and should not be confused
    with `unique_key`."""

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | str | None = None,
        label: str | None = None,
        session_id: str | None = None,
        unique_key: str | None = None,
        id: str | None = None,
        keep_url_fragment: bool = False,
        use_extended_unique_key: bool = False,
        always_enqueue: bool = False,
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
            headers: The HTTP headers of the request.
            payload: The data to be sent as the request body. Typically used with 'POST' or 'PUT' requests.
            label: A custom label to differentiate between request types. This is stored in `user_data`, and it is
                used for request routing (different requests go to different handlers).
            session_id: ID of a specific `Session` to which the request will be strictly bound.
                If the session becomes unavailable when the request is processed, a `RequestCollisionError` will be
                raised.
            unique_key: A unique key identifying the request. If not provided, it is automatically computed based on
                the URL and other parameters. Requests with the same `unique_key` are treated as identical.
            id: A unique identifier for the request. If not provided, it is automatically generated from the
                `unique_key`.
            keep_url_fragment: Determines whether the URL fragment (e.g., `#section`) should be included in
                the `unique_key` computation. This is only relevant when `unique_key` is not provided.
            use_extended_unique_key: Determines whether to include the HTTP method, ID Session and payload in the
                `unique_key` computation. This is only relevant when `unique_key` is not provided.
            always_enqueue: If set to `True`, the request will be enqueued even if it is already present in the queue.
                Using this is not allowed when a custom `unique_key` is also provided and will result in a `ValueError`.
            **kwargs: Additional request properties.
        """
        if unique_key is not None and always_enqueue:
            raise ValueError('`always_enqueue` cannot be used with a custom `unique_key`')

        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        if isinstance(payload, str):
            payload = payload.encode()

        unique_key = unique_key or compute_unique_key(
            url,
            method=method,
            headers=headers,
            payload=payload,
            session_id=session_id,
            keep_url_fragment=keep_url_fragment,
            use_extended_unique_key=use_extended_unique_key,
        )

        if always_enqueue:
            unique_key = f'{unique_key}_{crypto_random_object_id()}'

        id = id or unique_key_to_request_id(unique_key)

        request = cls(
            url=url,
            unique_key=unique_key,
            id=id,
            method=method,
            headers=headers,
            payload=payload,
            **kwargs,
        )

        if label is not None:
            request.user_data['label'] = label

        if session_id is not None:
            request.crawlee_data.session_id = session_id

        return request

    def get_query_param_from_url(self, param: str, *, default: str | None = None) -> str | None:
        """Get the value of a specific query parameter from the URL."""
        query_params = URL(self.url).query
        return query_params.get(param, default)

    @property
    def label(self) -> str | None:
        """A string used to differentiate between arbitrary request types."""
        return cast('UserData', self.user_data).label

    @property
    def session_id(self) -> str | None:
        """The ID of the bound session, if there is any."""
        return self.crawlee_data.session_id

    @property
    def crawlee_data(self) -> CrawleeRequestData:
        """Crawlee-specific configuration stored in the `user_data`."""
        user_data = cast('UserData', self.user_data)
        if user_data.crawlee_data is None:
            user_data.crawlee_data = CrawleeRequestData()

        return user_data.crawlee_data

    @property
    def crawl_depth(self) -> int:
        """The depth of the request in the crawl tree."""
        return self.crawlee_data.crawl_depth

    @crawl_depth.setter
    def crawl_depth(self, new_value: int) -> None:
        self.crawlee_data.crawl_depth = new_value

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
        """The strategy that was used for enqueuing the request."""
        return self.crawlee_data.enqueue_strategy or 'all'

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
        """Indicate whether the request should be enqueued at the front of the queue."""
        return self.crawlee_data.forefront

    @forefront.setter
    def forefront(self, new_value: bool) -> None:
        self.crawlee_data.forefront = new_value


class RequestWithLock(Request):
    """A crawling request with information about locks."""

    lock_expires_at: Annotated[datetime, Field(alias='lockExpiresAt')]
    """The timestamp when the lock expires."""
