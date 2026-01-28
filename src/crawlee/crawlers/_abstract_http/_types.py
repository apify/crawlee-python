from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from crawlee._types import HttpHeaders

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class CachedHttpResponse(BaseModel):
    """An `HttpResponse` implementation that serves pre-stored response data from cache."""

    http_version: str
    status_code: int
    headers: HttpHeaders
    body: bytes
    loaded_url: str | None = None

    async def read(self) -> bytes:
        return self.body

    async def read_stream(self) -> AsyncIterator[bytes]:
        yield self.body
