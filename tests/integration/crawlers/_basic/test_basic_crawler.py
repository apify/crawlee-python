from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal
from unittest.mock import AsyncMock, patch

import pytest

from crawlee._request import Request
from crawlee._utils.robots import RobotsTxtFile
from crawlee.crawlers import BasicCrawler
from crawlee.errors import (
    SessionError,
)
from crawlee.storages import RequestQueue

if TYPE_CHECKING:
    from yarl import URL

    from crawlee._types import BasicCrawlingContext, HttpMethod


@pytest.mark.parametrize(
    ('method', 'path', 'payload'),
    [
        pytest.param('GET', 'get', None, id='get send_request'),
        pytest.param('POST', 'post', b'Hello, world!', id='post send_request'),
    ],
)
async def test_send_request_works(server_url: URL, method: HttpMethod, path: str, payload: bytes | None) -> None:
    response_data: dict[str, Any] = {}

    crawler = BasicCrawler(max_request_retries=3)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        response = await context.send_request(str(server_url / path), method=method, payload=payload)

        response_data['body'] = json.loads(await response.read())
        response_data['headers'] = response.headers

    await crawler.run(['https://a.placeholder.com', 'https://b.placeholder.com', 'https://c.placeholder.com'])

    response_body = response_data.get('body')
    assert response_body is not None
    assert response_body.get('data') == (payload.decode() if payload else None)

    response_headers = response_data.get('headers')
    assert response_headers is not None
    content_type = response_headers.get('content-type')
    assert content_type is not None
    assert content_type == 'application/json'


async def test_send_request_respects_timeout(server_url: URL) -> None:
    request_timed_out = asyncio.Event()

    crawler = BasicCrawler(max_request_retries=3)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        with pytest.raises(TimeoutError):
            await context.send_request(
                str(server_url / 'slow') + '?delay=2',
                timeout=timedelta(milliseconds=100),
            )
        request_timed_out.set()

    await crawler.run(['https://a.placeholder.com'])

    assert request_timed_out.is_set()


async def test_session_rotation(server_url: URL) -> None:
    session_ids: list[str | None] = []

    crawler = BasicCrawler(
        max_session_rotations=7,
        max_request_retries=1,
    )

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        session_ids.append(context.session.id)
        raise SessionError('Test error')

    await crawler.run([str(server_url)])

    # exactly 7 handler calls happened
    assert len(session_ids) == 7

    # all session ids are not None
    assert None not in session_ids

    # and each was a different session
    assert len(set(session_ids)) == 7


@pytest.mark.parametrize(
    'skipped_at',
    [
        pytest.param('add_requests', id='skipped when added'),
        pytest.param('fetch', id='skipped when fetched from RQ'),
    ],
)
async def test_on_skipped_request_receives_request(
    server_url: URL, skipped_at: Literal['add_requests', 'fetch']
) -> None:
    """A request disallowed by robots.txt reaches the skipped request callback with its user data."""
    queue = await RequestQueue.open()
    crawler = BasicCrawler(request_manager=queue, respect_robots_txt_file=True, request_handler=AsyncMock())
    skipped_hook = AsyncMock()
    crawler.on_skipped_request(skipped_hook)
    request = Request.from_url(str(server_url / 'page_1'), user_data={'source': 'test'})

    if skipped_at == 'add_requests':
        await crawler.add_requests([request])
    else:
        await queue.add_request(request)
        await crawler.run()

    skipped_hook.assert_awaited_once()
    skipped_request, reason = skipped_hook.call_args.args
    assert isinstance(skipped_request, Request)
    assert skipped_request.url == request.url
    assert skipped_request.user_data['source'] == 'test'
    assert reason == 'robots_txt'


async def test_on_skipped_request_receives_request_for_url_string(server_url: URL) -> None:
    """A URL string passed to `add_requests` and disallowed by robots.txt reaches the callback as a `Request`."""
    crawler = BasicCrawler(respect_robots_txt_file=True)
    skipped_hook = AsyncMock()
    crawler.on_skipped_request(skipped_hook)
    url = str(server_url / 'page_1')

    await crawler.add_requests([url])

    skipped_hook.assert_awaited_once()
    skipped_request, reason = skipped_hook.call_args.args
    assert isinstance(skipped_request, Request)
    assert skipped_request.url == url
    assert reason == 'robots_txt'


async def test_lock_with_get_robots_txt_file_for_url(server_url: URL) -> None:
    crawler = BasicCrawler(respect_robots_txt_file=True)

    with patch('crawlee.crawlers._basic._basic_crawler.RobotsTxtFile.find', wraps=RobotsTxtFile.find) as spy:
        await asyncio.gather(
            *[asyncio.create_task(crawler._get_robots_txt_file_for_url(str(server_url))) for _ in range(10)]
        )

        # Check that the lock was acquired only once
        assert spy.call_count == 1
