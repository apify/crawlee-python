# ruff: noqa: TCH001, TCH002, TCH003

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Generic

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from typing_extensions import Self, TypeVar

from crawlee._utils.requests import compute_unique_key, unique_key_to_request_id
from crawlee._utils.urls import extract_query_params, validate_http_url
from crawlee.enqueue_strategy import EnqueueStrategy
from crawlee.types import HttpMethod


class BaseRequestData(BaseModel):
    """Data needed to create a new crawling request."""

    model_config = ConfigDict(populate_by_name=True)

    url: Annotated[str, BeforeValidator(validate_http_url), Field()]
    """URL of the web page to crawl"""

    unique_key: Annotated[str, Field(alias='uniqueKey')]
    """A unique key identifying the request. Two requests with the same `uniqueKey` are considered as pointing to the
    same URL.

    If `uniqueKey` is not provided, then it is automatically generated by normalizing the URL.
    For example, the URL of `HTTP://www.EXAMPLE.com/something/` will produce the `uniqueKey`
    of `http://www.example.com/something`.

    Pass an arbitrary non-empty text value to the `uniqueKey` property
    to override the default behavior and specify which URLs shall be considered equal.
    """

    method: HttpMethod = 'GET'

    payload: str | None = None

    headers: Annotated[dict[str, str] | None, Field(default_factory=dict)] = None

    query_params: Annotated[dict[str, Any] | None, Field(default_factory=dict)] = None

    data: Annotated[dict[str, Any] | None, Field(default_factory=dict)] = None

    user_data: Annotated[dict[str, Any], Field(alias='userData', default_factory=dict)]
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
        label: str | None = None,
        unique_key: str | None = None,
        **kwargs: Any,
    ) -> Self:
        """Create a new `RequestData` instance from a URL."""
        unique_key = unique_key or compute_unique_key(url)
        result = cls(url=url, unique_key=unique_key, **kwargs)

        if label is not None:
            result.user_data['label'] = label

        return result

    def get_query_param_from_url(self, param: str, *, default: str | None = None) -> str | None:
        """Get the value of a specific query parameter from the URL."""
        query_params = extract_query_params(self.url)
        values = query_params.get(param, [default])  # parse_qs returns values as list
        return values[0]


class Request(BaseRequestData):
    """A crawling request (as returned from a request queue)."""

    id: str

    json_: str | None = None  # TODO: get rid of this
    # https://github.com/apify/crawlee-py/issues/94

    order_no: Decimal | None = None  # TODO: get rid of this
    # https://github.com/apify/crawlee-py/issues/94

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        label: str | None = None,
        unique_key: str | None = None,
        id: str | None = None,
        **kwargs: Any,
    ) -> Self:
        """Create a new `RequestData` instance from a URL."""
        unique_key = unique_key or compute_unique_key(url)
        id = id or unique_key_to_request_id(unique_key)

        result = cls(url=url, unique_key=unique_key, id=id, **kwargs)

        if label is not None:
            result.user_data['label'] = label

        return result

    @classmethod
    def from_base_request_data(cls, base_request_data: BaseRequestData, *, id: str | None = None) -> Self:
        """Create a complete Request object based on a BaseRequestData instance."""
        kwargs = base_request_data.model_dump()
        kwargs['id'] = id or unique_key_to_request_id(base_request_data.unique_key)
        return cls(**kwargs)

    @property
    def label(self) -> str | None:
        """A string used to differentiate between arbitrary request types."""
        if 'label' in self.user_data:
            return str(self.user_data['label'])
        return None

    @property
    def crawlee_data(self) -> CrawleeRequestData:
        """Crawlee-specific configuration stored in the user_data."""
        return CrawleeRequestData.model_validate(self.user_data.get('__crawlee', {}))

    @property
    def state(self) -> RequestState | None:
        """Crawlee-specific request handling state."""
        return self.crawlee_data.state

    @state.setter
    def state(self, new_state: RequestState) -> None:
        self.user_data.setdefault('__crawlee', {})
        self.user_data['__crawlee']['state'] = new_state

    @property
    def max_retries(self) -> int | None:
        """Crawlee-specific limit on the number of retries of the request."""
        return self.crawlee_data.max_retries

    @max_retries.setter
    def max_retries(self, new_max_retries: int) -> None:
        self.user_data.setdefault('__crawlee', {})
        self.user_data['__crawlee']['maxRetries'] = new_max_retries

    @property
    def session_rotation_count(self) -> int | None:
        """Crawlee-specific number of finished session rotations for the request."""
        return self.crawlee_data.session_rotation_count

    @session_rotation_count.setter
    def session_rotation_count(self, new_session_rotation_count: int) -> None:
        self.user_data.setdefault('__crawlee', {})
        self.user_data['__crawlee']['sessionRotationCount'] = new_session_rotation_count

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
        self.user_data.setdefault('__crawlee', {})
        self.user_data['__crawlee']['enqueueStrategy'] = str(new_enqueue_strategy)

    @property
    def last_proxy_tier(self) -> int | None:
        """The last proxy tier used to process the request."""
        return self.crawlee_data.last_proxy_tier

    @last_proxy_tier.setter
    def last_proxy_tier(self, new_value: int) -> None:
        self.user_data.setdefault('__crawlee', {})
        self.user_data['__crawlee']['lastProxyTier'] = new_value

    @property
    def forefront(self) -> bool:
        """Should the request be enqueued at the start of the queue?"""
        return self.crawlee_data.forefront

    @forefront.setter
    def forefront(self, new_value: bool) -> None:
        self.user_data.setdefault('__crawlee', {})
        self.user_data['__crawlee']['forefront'] = new_value


class RequestWithLock(Request):
    """A crawling request with information about locks."""

    lock_expires_at: Annotated[datetime, Field(alias='lockExpiresAt')]


class RequestState(Enum):
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
    """Crawlee-specific configuration stored in the user_data."""

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


class BaseStorageMetadata(BaseModel):
    """Base model for storage metadata."""

    model_config = ConfigDict(populate_by_name=True)

    id: Annotated[str, Field(alias='id')]
    name: Annotated[str | None, Field(alias='name', default='')]
    accessed_at: Annotated[datetime, Field(alias='accessedAt')]
    created_at: Annotated[datetime, Field(alias='createdAt')]
    modified_at: Annotated[datetime, Field(alias='modifiedAt')]


class DatasetMetadata(BaseStorageMetadata):
    """Model for a dataset metadata."""

    model_config = ConfigDict(populate_by_name=True)

    item_count: Annotated[int, Field(alias='itemCount')]


class KeyValueStoreMetadata(BaseStorageMetadata):
    """Model for a key-value store metadata."""

    model_config = ConfigDict(populate_by_name=True)

    user_id: Annotated[str, Field(alias='userId')]


class RequestQueueMetadata(BaseStorageMetadata):
    """Model for a request queue metadata."""

    model_config = ConfigDict(populate_by_name=True)

    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]
    handled_request_count: Annotated[int, Field(alias='handledRequestCount')]
    pending_request_count: Annotated[int, Field(alias='pendingRequestCount')]
    stats: Annotated[dict, Field(alias='stats')]
    total_request_count: Annotated[int, Field(alias='totalRequestCount')]
    user_id: Annotated[str, Field(alias='userId')]
    resource_directory: Annotated[str, Field(alias='resourceDirectory')]


ValueType = TypeVar('ValueType', default=Any)


class KeyValueStoreRecord(BaseModel, Generic[ValueType]):
    """Model for a key-value store record."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    value: Annotated[ValueType, Field(alias='value')]
    content_type: Annotated[str | None, Field(alias='contentType', default=None)]
    filename: Annotated[str | None, Field(alias='filename', default=None)]


class KeyValueStoreRecordMetadata(BaseModel):
    """Model for a key-value store record metadata."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    content_type: Annotated[str, Field(alias='contentType')]


class KeyValueStoreKeyInfo(BaseModel):
    """Model for a key-value store key info."""

    model_config = ConfigDict(populate_by_name=True)

    key: Annotated[str, Field(alias='key')]
    size: Annotated[int, Field(alias='size')]


class KeyValueStoreListKeysPage(BaseModel):
    """Model for listing keys in the key-value store."""

    model_config = ConfigDict(populate_by_name=True)

    count: Annotated[int, Field(alias='count')]
    limit: Annotated[int, Field(alias='limit')]
    is_truncated: Annotated[bool, Field(alias='isTruncated')]
    items: Annotated[list[KeyValueStoreKeyInfo], Field(alias='items', default_factory=list)]
    exclusive_start_key: Annotated[str | None, Field(alias='exclusiveStartKey', default=None)]
    next_exclusive_start_key: Annotated[str | None, Field(alias='nextExclusiveStartKey', default=None)]


class RequestQueueHeadState(BaseModel):
    """Model for the request queue head state."""

    model_config = ConfigDict(populate_by_name=True)

    was_limit_reached: Annotated[bool, Field(alias='wasLimitReached')]
    prev_limit: Annotated[int, Field(alias='prevLimit')]
    queue_modified_at: Annotated[datetime, Field(alias='queueModifiedAt')]
    query_started_at: Annotated[datetime, Field(alias='queryStartedAt')]
    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]


class RequestQueueHead(BaseModel):
    """Model for the request queue head."""

    model_config = ConfigDict(populate_by_name=True)

    limit: Annotated[int | None, Field(alias='limit', default=None)]
    had_multiple_clients: Annotated[bool, Field(alias='hadMultipleClients')]
    queue_modified_at: Annotated[datetime, Field(alias='queueModifiedAt')]
    items: Annotated[list[Request], Field(alias='items', default_factory=list)]


class RequestQueueHeadWithLocks(RequestQueueHead):
    """Model for request queue head with locks."""

    lock_secs: Annotated[int, Field(alias='lockSecs')]
    items: Annotated[list[Request], Field(alias='items', default_factory=list)]


class BaseListPage(BaseModel):
    """Model for a single page of storage items returned from a collection list method.

    Args:
        count: Count of the returned objects on this page.
        offset: The offset of the first object specified in the API call.
        limit: The limit on the number of returned objects specified in the API call.
        total: Total number of objects matching the API call criteria.
        desc: Whether the listing is descending or not.
    """

    model_config = ConfigDict(populate_by_name=True)

    count: Annotated[int, Field(default=0)]
    offset: Annotated[int, Field(default=0)]
    limit: Annotated[int, Field(default=0)]
    total: Annotated[int, Field(default=0)]
    desc: Annotated[bool, Field(default=False)]


class DatasetListPage(BaseListPage):
    """Model for a single page of dataset items returned from a collection list method.

    Args:
        items: List of returned dataset items on this page.
    """

    items: Annotated[list[DatasetMetadata], Field(default_factory=list)]


class KeyValueStoreListPage(BaseListPage):
    """Model for a single page of key-value store items returned from a collection list method.

    Args:
        items: List of returned key-value store items on this page.
    """

    items: Annotated[list[KeyValueStoreMetadata], Field(default_factory=list)]


class RequestQueueListPage(BaseListPage):
    """Model for a single page of request queue items returned from a collection list method.

    Args:
        items: List of returned request queue items on this page.
    """

    items: Annotated[list[RequestQueueMetadata], Field(default_factory=list)]


class DatasetItemsListPage(BaseListPage):
    """Model for a single page of dataset items returned from a collection list method.

    Args:
        items: List of returned dataset items on this page.
    """

    items: Annotated[list[dict], Field(default_factory=list)]


class ProlongRequestLockResponse(BaseModel):
    """Response to prolong request lock calls."""

    model_config = ConfigDict(populate_by_name=True)

    lock_expires_at: Annotated[datetime, Field(alias="'lockExpiresAt'")]


class ProcessedRequest(BaseModel):
    """Represents a processed request."""

    model_config = ConfigDict(populate_by_name=True)

    id: Annotated[str, Field(alias='id')]
    unique_key: Annotated[str, Field(alias='uniqueKey')]
    was_already_present: Annotated[bool, Field(alias='wasAlreadyPresent')]
    was_already_handled: Annotated[bool, Field(alias='wasAlreadyHandled')]


class UnprocessedRequest(BaseModel):
    """Represents an unprocessed request."""

    model_config = ConfigDict(populate_by_name=True)

    unique_key: Annotated[str, Field(alias='requestUniqueKey')]
    url: Annotated[str, BeforeValidator(validate_http_url), Field()]
    method: Annotated[HttpMethod | None, Field()] = None


class BatchRequestsOperationResponse(BaseModel):
    """Response to batch request deletion calls."""

    model_config = ConfigDict(populate_by_name=True)

    processed_requests: Annotated[list[ProcessedRequest], Field(alias='processedRequests')]
    unprocessed_requests: Annotated[list[UnprocessedRequest], Field(alias='unprocessedRequests')]


class RequestListResponse(BaseModel):
    """Response to a request list call."""

    model_config = ConfigDict(populate_by_name=True)

    limit: Annotated[int, Field()]
    exclusive_start_key: Annotated[str | None, Field(alias='exclusiveStartId')]
    items: Annotated[list[Request], Field()]
