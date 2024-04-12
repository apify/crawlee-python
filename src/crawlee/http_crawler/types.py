from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee.basic_crawler.types import BasicCrawlingContext

if TYPE_CHECKING:
    from httpx import Response


@dataclass(frozen=True)
class HttpCrawlingContext(BasicCrawlingContext):
    """HTTP crawling context."""

    http_response: Response
