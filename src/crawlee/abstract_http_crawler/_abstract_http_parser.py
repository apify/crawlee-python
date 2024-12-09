from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic

from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.docs import docs_group
from crawlee.abstract_http_crawler._http_crawling_context import TParseResult
from crawlee.basic_crawler import BlockedInfo

if TYPE_CHECKING:
    from collections.abc import Iterable

    from crawlee.http_clients import HttpResponse


@docs_group('Abstract classes')
class AbstractHttpParser(Generic[TParseResult], ABC):
    """Parser used for parsing http response and inspecting parsed result to find links or detect blocking."""

    @abstractmethod
    async def parse(self, response: HttpResponse) -> TParseResult:
        """Parse http response.

        Args:
            response: HTTP response to be parsed.

        Returns:
            Parsed HTTP response.
        """

    def is_blocked(self, parsed_content: TParseResult) -> BlockedInfo:
        """Detect if blocked and return BlockedInfo with additional information.

        Default implementation that expects `is_matching_selector` abstract method to be implemented.
        Override this method if your parser has different way of blockage detection.

        Args:
            parsed_content: Parsed HTTP response. Result of `parse` method.

        Returns:
            `BlockedInfo` object that contains non-empty string description of reason if blockage was detected. Empty
            string in reason signifies no blockage detected.
        """
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
        """Find if selector has match in parsed content.

        Args:
            parsed_content: Parsed HTTP response. Result of `parse` method.
            selector: String used to define matching pattern.

        Returns:
            True if selector has match in parsed content.
        """

    @abstractmethod
    def find_links(self, parsed_content: TParseResult, selector: str) -> Iterable[str]:
        """Find all links in result using selector.

        Args:
            parsed_content: Parsed HTTP response. Result of `parse` method.
            selector: String used to define matching pattern for finding links.

        Returns:
            Iterable of strings that contain found links.
        """
