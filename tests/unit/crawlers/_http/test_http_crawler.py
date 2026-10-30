from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock, call

import pytest

from crawlee.crawlers import HttpCrawler
from crawlee.http_clients import HttpClient, HttpResponse
from crawlee.sessions import SessionPool
from tests._utils import open_throttler_held_until_reclaim

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from crawlee.crawlers import HttpCrawlingContext


@pytest.fixture
async def crawler_without_retries(
    mock_request_handler: Callable[[HttpCrawlingContext], Awaitable[None]],
) -> HttpCrawler:
    return HttpCrawler(
        request_handler=mock_request_handler,
        retry_on_blocked=False,
        max_request_retries=0,
    )


def test_default_logger() -> None:
    assert HttpCrawler().log.name == 'HttpCrawler'


def mock_response(status_code: int, url: str) -> Mock:
    """Build an HTTP response with the given status code and an empty body."""
    return Mock(spec=HttpResponse, status_code=status_code, headers={}, url=url, read=AsyncMock(return_value=b''))


@pytest.mark.parametrize(
    ('status_code', 'expected_handler_calls'),
    [
        pytest.param(200, [call(closed=False)], id='handled'),
        pytest.param(500, [], id='error_status'),
    ],
)
async def test_navigation_response_closed_after_request(*, status_code: int, expected_handler_calls: list) -> None:
    """Test that the response of the navigation request stays open in the handler and is closed after the request."""
    url = 'https://a.placeholder.com'
    response = mock_response(status_code, url)
    http_client = AsyncMock(spec=HttpClient)
    http_client.send_request.return_value = response
    crawler = HttpCrawler(http_client=http_client, max_request_retries=0)
    handler_call = Mock()

    @crawler.router.default_handler
    async def handler(_context: HttpCrawlingContext) -> None:
        handler_call(closed=response.aclose.called)

    await crawler.run([url])

    assert handler_call.call_args_list == expected_handler_calls
    response.aclose.assert_awaited_once()


async def test_body_read_bound_by_navigation_timeout() -> None:
    """Test that reading the body of the navigation response fails once the navigation timeout runs out."""
    url = 'https://a.placeholder.com'
    response = mock_response(200, url)
    response.read.side_effect = asyncio.Event().wait
    http_client = AsyncMock(spec=HttpClient)
    http_client.send_request.return_value = response
    crawler = HttpCrawler(
        http_client=http_client,
        navigation_timeout=timedelta(milliseconds=100),
        max_request_retries=0,
    )
    handler = AsyncMock()
    crawler.router.default_handler(handler)
    failed_request_handler = AsyncMock()
    crawler.failed_request_handler(failed_request_handler)

    await crawler.run([url])

    handler.assert_not_called()
    assert isinstance(failed_request_handler.call_args.args[1], TimeoutError)


@pytest.mark.parametrize(
    'crawler_kwargs',
    [
        pytest.param({}, id='default'),
        pytest.param({'retry_on_blocked': False}, id='no_retry_on_blocked'),
        pytest.param({'ignore_http_error_status_codes': {429}}, id='ignored_429'),
    ],
)
async def test_throttled_429_is_deferred(crawler_kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """A throttled 429 is retried later without spending a retry, a session rotation or session reputation."""
    throttler = await open_throttler_held_until_reclaim(monkeypatch)
    http_client = AsyncMock(spec=HttpClient)
    url = 'https://throttled.placeholder.com/page'
    http_client.send_request.side_effect = [mock_response(429, url), mock_response(200, url)]
    failed_request_handler = AsyncMock()
    crawler = HttpCrawler(
        http_client=http_client,
        request_manager=throttler,
        session_pool=SessionPool(max_pool_size=1),
        max_request_retries=0,
        max_session_rotations=0,
        **crawler_kwargs,
    )
    crawler.failed_request_handler(failed_request_handler)
    handled: list[HttpCrawlingContext] = []

    @crawler.router.default_handler
    async def handler(context: HttpCrawlingContext) -> None:
        handled.append(context)

    await crawler.run([url])

    assert [context.http_response.status_code for context in handled] == [200]
    failed_request_handler.assert_not_called()
    request = handled[0].request
    assert request.retry_count == 0
    assert (request.session_rotation_count or 0) == 0
    session = handled[0].session
    assert session is not None
    assert session.error_score == 0
    assert session.is_usable
