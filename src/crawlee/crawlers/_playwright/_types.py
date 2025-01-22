from __future__ import annotations

from typing import Protocol


@docs_group('Functions')
class BlockRequestsFunction(Protocol):
    """Protocol defining the interface for block_requests function."""

    async def __call__(
        self, url_patterns: list[str] | None = None, extra_url_patterns: list[str] | None = None
    ) -> None:
        """Blocks network requests matching specified URL patterns. Works only for Chromium browser.

        Args:
            url_patterns: List of URL patterns to block. If None, uses default patterns.
            extra_url_patterns: Additional URL patterns to append to the main patterns list.
        """
        ...
