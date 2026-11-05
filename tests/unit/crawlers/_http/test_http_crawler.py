from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee.crawlers import HttpCrawler
from crawlee.http_clients import HttpClient, HttpCrawlingResult
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


def crawl_result(status_code: int) -> HttpCrawlingResult:
    """Build an HTTP client result with the given status code and an empty body."""
    return HttpCrawlingResult(http_response=Mock(status_code=status_code, headers={}, read=AsyncMock(return_value=b'')))


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
    http_client.crawl.side_effect = [crawl_result(429), crawl_result(200)]
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

    await crawler.run(['https://throttled.placeholder.com/page'])

    assert [context.http_response.status_code for context in handled] == [200]
    failed_request_handler.assert_not_called()
    request = handled[0].request
    assert request.retry_count == 0
    assert (request.session_rotation_count or 0) == 0
    session = handled[0].session
    assert session is not None
    assert session.error_score == 0
    assert session.is_usable
