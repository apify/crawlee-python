from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Iterable

from typing_extensions import override

from crawlee.http_crawler._http_crawling_context import TParseResult

if TYPE_CHECKING:
    from crawlee.http_clients import HttpResponse


@dataclass(frozen=True)
class BlockedInfo:
    """Information about whether the crawling is blocked. If reason is empty, then it means it is not blocked."""

    reason: str

    def __bool__(self) -> bool:
        """No reason means no blocking."""
        return bool(self.reason)


class StaticContentParser(Generic[TParseResult], ABC):
    """Parser used for parsing http response and inspecting parsed result to find links or detect blocking."""

    @abstractmethod
    async def parse(self, http_response: HttpResponse) -> TParseResult:
        """Parse http response."""
        ...

    @abstractmethod
    def is_blocked(self, parsed_content: TParseResult) -> BlockedInfo:
        """Detect if blocked and return BlockedInfo with additional information."""
        ...

    @abstractmethod
    def find_links(self, parsed_content: TParseResult, selector: str) -> Iterable[str]:
        """Find all links in result using selector."""
        ...


class NoParser(StaticContentParser[bytes]):
    """Dummy parser for backwards compatibility.

    To enable using HttpCrawler without need for additional specific parser.
    """

    @override
    async def parse(self, http_response: HttpResponse) -> bytes:
        return http_response.read()

    @override
    def is_blocked(self, parsed_content: bytes) -> BlockedInfo:  # Intentional unused argument.
        return BlockedInfo(reason='')

    @override
    def find_links(self, parsed_content: bytes, selector: str) -> Iterable[str]:  # Intentional unused argument.
        return []
