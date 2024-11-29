from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.static_content_crawler._static_content_parser import BlockedInfo, StaticContentParser

if TYPE_CHECKING:
    from collections.abc import Iterable

    from crawlee.http_clients import HttpResponse


class NoParser(StaticContentParser[bytes]):
    """Dummy parser for backwards compatibility.

    To enable using HttpCrawler without need for additional specific parser.
    """

    @override
    async def parse(self, response: HttpResponse) -> bytes:
        return response.read()

    @override
    def is_blocked(self, _: bytes) -> BlockedInfo:
        return BlockedInfo(reason='')

    @override
    def is_matching_selector(self, _: bytes, selector: str) -> bool:
        return False

    @override
    def find_links(self, _: bytes, selector: str) -> Iterable[str]:
        return []
