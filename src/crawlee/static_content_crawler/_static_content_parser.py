from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic

from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee.static_content_crawler._static_crawling_context import TParseResult

if TYPE_CHECKING:
    from collections.abc import Iterable

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
    async def parse(self, response: HttpResponse) -> TParseResult:
        """Parse http response."""

    def is_blocked(self, parsed_content: TParseResult) -> BlockedInfo:
        """Detect if blocked and return BlockedInfo with additional information."""
        reason = ''
        if parsed_content is not None:
            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if self.is_matching_selector(parsed_content, selector)
            ]

            if matched_selectors:
                reason = (
                    f"Assuming the session is blocked - HTTP response matched the following selectors: "
                    f"{'; '.join(matched_selectors)}"
                )

        return BlockedInfo(reason=reason)

    @abstractmethod
    def is_matching_selector(self, parsed_content: TParseResult, selector: str) -> bool:
        """Find if selector has match in parsed content."""

    @abstractmethod
    def find_links(self, parsed_content: TParseResult, selector: str) -> Iterable[str]:
        """Find all links in result using selector."""
