from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class RequestData(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    url: str
    """URL of the web page to crawl. It must be a non-empty string."""

    loaded_url: Annotated[str | None, Field(alias='loadedUrl')] = None

    unique_key: Annotated[str, Field(alias='uniqueKey')]
    """A unique key identifying the request. Two requests with the same `uniqueKey` are considered as pointing to the
    same URL.

    If `uniqueKey` is not provided, then it is automatically generated by normalizing the URL.
    For example, the URL of `HTTP://www.EXAMPLE.com/something/` will produce the `uniqueKey`
    of `http://www.example.com/something`.

    Pass an arbitrary non-empty text value to the `uniqueKey` property
    to override the default behavior and specify which URLs shall be considered equal.
    """

    method: str = 'get'

    payload: str | None = None

    headers: Annotated[dict[str, str] | None, Field(default_factory=dict)] = None

    user_data: Annotated[dict[str, Any] | None, Field(alias='userData')]
    """Custom user data assigned to the request. Use this to save any request related data to the
    request's scope, keeping them accessible on retries, failures etc.
    """

    retry_count: Annotated[int, Field(alias='retryCount')] = 0

    no_retry: Annotated[bool, Field(alias='noRetry')] = False

    id: str

    handled_at: Annotated[datetime | None, Field(alias='handledAt')] = None

    @property
    def crawlee_data(self: RequestData) -> CrawleeRequestData:
        return CrawleeRequestData.model_validate(self.user_data.get('__crawlee', {}) if self.user_data else {})

    @property
    def label(self: RequestData) -> str | None:
        if self.user_data and 'label' in self.user_data:
            return str(self.user_data['label'])
        return None


class CrawleeRequestData(BaseModel):
    max_retries: Annotated[int | None, Field(alias='maxRetries')] = None
    """Maximum number of retries for this request. Allows to override the global `maxRequestRetries` option of
    `BasicCrawler`."""

    enqueue_strategy: Annotated[str | None, Field(alias='enqueueStrategy')] = None

    state: str | None = None
    """Describes the request's current lifecycle state."""

    session_rotation_count: Annotated[int | None, Field(alias='sessionRotationCount')] = None

    skip_navigation: Annotated[bool, Field(alias='skipNavigation')] = False


class BasicCrawlingContext(Protocol):
    @property
    def request(self: BasicCrawlingContext) -> RequestData:
        ...
