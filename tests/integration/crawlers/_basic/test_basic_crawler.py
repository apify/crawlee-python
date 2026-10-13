from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from crawlee._utils.robots import RobotsTxtFile
from crawlee.crawlers import BasicCrawler
from crawlee.errors import (
    SessionError,
)

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
        session_ids.append(context.session.id if context.session else None)
        raise SessionError('Test error')

    await crawler.run([str(server_url)])

    # exactly 7 handler calls happened
    assert len(session_ids) == 7

    # all session ids are not None
    assert None not in session_ids

    # and each was a different session
    assert len(set(session_ids)) == 7


async def test_lock_with_get_robots_txt_file_for_url(server_url: URL) -> None:
    crawler = BasicCrawler(respect_robots_txt_file=True)

    with patch('crawlee.crawlers._basic._basic_crawler.RobotsTxtFile.find', wraps=RobotsTxtFile.find) as spy:
        await asyncio.gather(
            *[asyncio.create_task(crawler._get_robots_txt_file_for_url(str(server_url))) for _ in range(10)]
        )

        # Check that the lock was acquired only once
        assert spy.call_count == 1
