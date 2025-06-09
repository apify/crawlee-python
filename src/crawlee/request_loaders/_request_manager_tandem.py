from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.request_loaders import RequestManager

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Request
    from crawlee.request_loaders import RequestLoader
    from crawlee.storage_clients.models import ProcessedRequest


logger = getLogger(__name__)


@docs_group('Classes')
class RequestManagerTandem(RequestManager):
    """Implements a tandem behaviour for a pair of `RequestLoader` and `RequestManager`.

    In this scenario, the contents of the "loader" get transferred into the "manager", allowing processing the requests
    from both sources and also enqueueing new requests (not possible with plain `RequestManager`).
    """

    def __init__(self, request_loader: RequestLoader, request_manager: RequestManager) -> None:
        self._read_only_loader = request_loader
        self._read_write_manager = request_manager

    @override
    async def get_handled_count(self) -> int:
        return await self._read_write_manager.get_handled_count()

    @override
    async def get_total_count(self) -> int:
        return (await self._read_only_loader.get_total_count()) + (await self._read_write_manager.get_total_count())

    @override
    async def is_empty(self) -> bool:
        return (await self._read_only_loader.is_empty()) and (await self._read_write_manager.is_empty())

    @override
    async def is_finished(self) -> bool:
        return (await self._read_only_loader.is_finished()) and (await self._read_write_manager.is_finished())

    @override
    async def add_request(self, request: str | Request, *, forefront: bool = False) -> ProcessedRequest:
        return await self._read_write_manager.add_request(request, forefront=forefront)

    @override
    async def add_requests(
        self,
        requests: Sequence[str | Request],
        *,
        forefront: bool = False,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        return await self._read_write_manager.add_requests(
            requests,
            forefront=forefront,
            batch_size=batch_size,
            wait_time_between_batches=wait_time_between_batches,
            wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
            wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
        )

    @override
    async def fetch_next_request(self) -> Request | None:
        if await self._read_only_loader.is_finished():
            return await self._read_write_manager.fetch_next_request()

        request = await self._read_only_loader.fetch_next_request()

        if not request:
            return await self._read_write_manager.fetch_next_request()

        try:
            await self._read_write_manager.add_request(request, forefront=True)
        except Exception:
            logger.exception(
                'Adding request from the RequestLoader to the RequestManager failed, the request has been dropped',
                extra={'url': request.url, 'unique_key': request.unique_key},
            )
            return None

        await self._read_only_loader.mark_request_as_handled(request)

        return await self._read_write_manager.fetch_next_request()

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> None:
        await self._read_write_manager.reclaim_request(request, forefront=forefront)

    @override
    async def mark_request_as_handled(self, request: Request) -> None:
        await self._read_write_manager.mark_request_as_handled(request)

    @override
    async def drop(self) -> None:
        await self._read_write_manager.drop()
