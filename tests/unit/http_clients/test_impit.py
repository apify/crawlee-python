from __future__ import annotations

import json
from typing import TYPE_CHECKING

from crawlee.http_clients import ImpitHttpClient
from crawlee.sessions import Session

if TYPE_CHECKING:
    from yarl import URL


async def test_cleanup_clears_client_cache(server_url: URL) -> None:
    """`ImpitHttpClient.cleanup` must drop cached clients so the next request creates a fresh one."""
    client = ImpitHttpClient()
    async with client:
        await client.send_request(str(server_url))
        assert len(client._client_cache) == 1
        first_client = next(iter(client._client_cache.values()))

        await client.cleanup()
        assert len(client._client_cache) == 0

        await client.send_request(str(server_url))
        assert len(client._client_cache) == 1
        second_client = next(iter(client._client_cache.values()))
        assert second_client is not first_client


async def test_persist_false_still_sends_session_cookies(server_url: URL) -> None:
    """When persist_cookies_per_session=False, pre-seeded session cookies are still sent via Cookie header."""
    client = ImpitHttpClient(persist_cookies_per_session=False)
    session = Session()
    session.cookies.set('seed', 'value123', domain=server_url.host or '127.0.0.1', path='/')

    async with client:
        response = await client.send_request(str(server_url / 'cookies'), session=session)
        body = json.loads(await response.read())

    assert body['cookies'] == {'seed': 'value123'}
