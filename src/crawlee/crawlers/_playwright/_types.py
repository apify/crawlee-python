from __future__ import annotations

from typing import Protocol

from crawlee._utils.docs import docs_group


@docs_group('Functions')
class BlockRequestsFunction(Protocol):
    """A function for blocking unwanted HTTP requests during page loads in PlaywrightCrawler.

    It simplifies the process of blocking specific HTTP requests during page navigation.
    The function allows blocking both default resource types (like images, fonts, stylesheets) and custom URL patterns.
    """

    async def __call__(
        self, url_patterns: list[str] | None = None, extra_url_patterns: list[str] | None = None
    ) -> None:
        """Call dunder method.

        Args:
            url_patterns: List of URL patterns to block. If None, uses default patterns.
            extra_url_patterns: Additional URL patterns to append to the main patterns list.
        """
