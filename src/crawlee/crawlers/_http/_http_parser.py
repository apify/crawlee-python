from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.crawlers._abstract_http import AbstractHttpParser
from crawlee.crawlers._types import BlockedInfo

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from crawlee.http_clients import HttpResponse


class NoParser(AbstractHttpParser[bytes, bytes]):
    """Dummy parser for backwards compatibility.

    To enable using `HttpCrawler` without need for additional specific parser.
    """

    @override
    async def parse(self, response: HttpResponse) -> bytes:
        return await response.read()

    @override
    async def parse_text(self, text: str) -> bytes:
        raise NotImplementedError

    @override
    async def select(self, parsed_content: bytes, selector: str) -> Sequence[bytes]:
        raise NotImplementedError

    @override
    def is_blocked(self, parsed_content: bytes) -> BlockedInfo:  # Intentional unused argument.
        return BlockedInfo(reason='')

    @override
    def is_matching_selector(self, parsed_content: bytes, selector: str) -> bool:  # Intentional unused argument.
        return False

    @override
    def find_links(self, parsed_content: bytes, selector: str) -> Iterable[str]:  # Intentional unused argument.
        return []
