from dataclasses import dataclass

from httpx import Response

from crawlee.basic_crawler.types import BasicCrawlingContext


@dataclass(frozen=True)
class HttpCrawlingContext(BasicCrawlingContext):
    """HTTP crawling context."""

    http_response: Response
