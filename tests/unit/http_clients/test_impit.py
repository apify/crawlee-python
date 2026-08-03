from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.http_clients import ImpitHttpClient

if TYPE_CHECKING:
    from yarl import URL


async def test_cleanup_clears_client_cache(server_url: URL) -> None:
    """`ImpitHttpClient.cleanup` must drop cached clients so the next request creates a fresh one."""
    client = ImpitHttpClient()
    async with client:
        await client.send_request(str(server_url))
        assert len(client._client_cache) == 1
        first_client = next(iter(client._client_cache.values()))['client']

        await client.cleanup()
        assert len(client._client_cache) == 0

        await client.send_request(str(server_url))
        assert len(client._client_cache) == 1
        second_client = next(iter(client._client_cache.values()))['client']
        assert second_client is not first_client
