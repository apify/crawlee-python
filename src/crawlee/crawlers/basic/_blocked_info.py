from __future__ import annotations

from dataclasses import dataclass

from crawlee._utils.docs import docs_group


@docs_group('Data structures')
@dataclass(frozen=True)
class BlockedInfo:
    """Information about whether the crawling is blocked. If reason is empty, then it means it is not blocked."""

    reason: str

    def __bool__(self) -> bool:
        """No reason means no blocking."""
        return bool(self.reason)
