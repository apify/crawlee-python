# ruff: noqa: TCH003
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.types import Request


@dataclass(frozen=True)
class BasicCrawlingContext:
    """Basic crawling context intended to be extended by crawlers."""

    request: Request


@dataclass(frozen=True)
class FinalStatistics:
    """Statistics about a crawler run."""
