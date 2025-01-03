from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BlockedInfo:
    """Information about whether the crawling is blocked. If reason is empty, then it means it is not blocked."""

    reason: str

    def __bool__(self) -> bool:
        """No reason means no blocking."""
        return bool(self.reason)
