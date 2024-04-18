# ruff: noqa: TCH003
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Coroutine, Protocol

if TYPE_CHECKING:
    from crawlee.http_clients.base_http_client import HttpResponse
    from crawlee.request import Request


class SendRequestFunction(Protocol):
    """Type of a function for performing an HTTP request."""

    def __call__(  # noqa: D102
        self, url: str, *, method: str = 'get', headers: dict[str, str] | None = None
    ) -> Coroutine[None, None, HttpResponse]: ...


@dataclass(frozen=True)
class BasicCrawlingContext:
    """Basic crawling context intended to be extended by crawlers."""

    request: Request
    send_request: SendRequestFunction


@dataclass(frozen=True)
class FinalStatistics:
    """Statistics about a crawler run."""
