# ruff: noqa: TCH003

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from crawlee._utils.requests import compute_unique_key, unique_key_to_request_id


class Request(BaseModel):
    """A crawling request body."""

    model_config = ConfigDict(populate_by_name=True)

    url: str

    unique_key: Annotated[str, Field(alias='uniqueKey')]

    id_: Annotated[str, Field(alias='id')]

    method: str = 'get'

    payload: str | None = None

    headers: Annotated[dict[str, str] | None, Field(default_factory=dict)] = None

    json_: str | None = None

    order_no: Decimal | None = None

    user_data: Annotated[dict[str, Any] | None, Field(alias='userData')] = None

    retry_count: Annotated[int, Field(alias='retryCount')] = 0

    no_retry: Annotated[bool, Field(alias='noRetry')] = False

    loaded_url: Annotated[str | None, Field(alias='loadedUrl')] = None

    handled_at: Annotated[datetime | None, Field(alias='handledAt')] = None

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        unique_key: str | None = None,
        id_: str | None = None,
        **kwargs: Any,
    ) -> Request:
        """Create a new `RequestData` instance from a URL."""
        unique_key = unique_key or compute_unique_key(url)
        id_ = id_ or unique_key_to_request_id(unique_key)
        return cls(url=url, unique_key=unique_key, id_=id_, **kwargs)
