from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.storages._request_provider import RequestProvider

if TYPE_CHECKING:
    from crawlee import Request
    from crawlee.storages._request_source import RequestSource


logger = getLogger(__name__)


class RequestSourceTandem(RequestProvider):
    """Implements a tandem behaviour for a pair of `RequestSource` and `RequestProvider`.

    In this scenario, the contents of the "source" get transferred into the "provider", allowing processing the requests
    from both sources and also enqueueing new requests (not possible with plain `RequestSource`).
    """

    def __init__(self, request_source: RequestSource, request_provider: RequestProvider) -> None:
        self._read_only_source = request_source
        self._read_write_provider = request_provider

    @property
    @override
    def name(self) -> str:
        return ''

    @override
    async def get_total_count(self) -> int:
        return (await self._read_only_source.get_total_count()) + (await self._read_write_provider.get_total_count())

    @override
    async def is_empty(self) -> bool:
        return (await self._read_only_source.is_empty()) and (await self._read_write_provider.is_empty())

    @override
    async def is_finished(self) -> bool:
        return (await self._read_only_source.is_finished()) and (await self._read_write_provider.is_finished())

    @override
    async def fetch_next_request(self) -> Request | None:
        if await self._read_only_source.is_finished():
            return await self._read_write_provider.fetch_next_request()

        request = await self._read_only_source.fetch_next_request()

        if not request:
            return await self._read_write_provider.fetch_next_request()

        try:
            await self._read_write_provider.add_request(request, forefront=True)
        except Exception:
            logger.exception(
                'Adding request from the RequestList to the RequestQueue failed, reclaiming request back to the list.',
            )
            await self._read_only_source.reclaim_request(request)
            return None

        await self._read_only_source.mark_request_as_handled(request)

        return await self._read_write_provider.fetch_next_request()

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> None:
        await self.reclaim_request(request, forefront=forefront)

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        await self._read_write_provider.mark_request_as_handled(request)

    @override
    async def get_handled_count(self) -> int:
        return await self._read_write_provider.get_handled_count()
