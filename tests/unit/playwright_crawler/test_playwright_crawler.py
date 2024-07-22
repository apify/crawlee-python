# TODO: The current PlaywrightCrawler tests rely on external websites. It means they can fail or take more time
# due to network issues. To enhance test stability and reliability, we should mock the network requests.
# https://github.com/apify/crawlee-python/issues/197

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from crawlee import Glob
from crawlee.playwright_crawler import PlaywrightCrawler
from crawlee.storages import RequestList

if TYPE_CHECKING:
    from crawlee.playwright_crawler import PlaywrightCrawlingContext


async def test_basic_request(httpbin: str) -> None:
    requests = [f'{httpbin}/']
    crawler = PlaywrightCrawler()
    result: dict = {}

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        assert context.page is not None
        result['request_url'] = str(context.request.url)
        result['page_url'] = context.page.url
        result['page_title'] = await context.page.title()
        result['page_content'] = await context.page.content()

    await crawler.run(requests)

    assert result.get('request_url') == result.get('page_url') == f'{httpbin}/'
    assert 'httpbin' in result.get('page_title', '')
    assert '<html' in result.get('page_content', '')  # there is some HTML content


async def test_enqueue_links() -> None:
    requests = ['https://crawlee.dev/docs/examples']
    crawler = PlaywrightCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        url = str(context.request.url)
        visit(url)
        await context.enqueue_links(include=[Glob('https://crawlee.dev/docs/examples/**')])

    await crawler.run(requests)

    visited: set[str] = {call[0][0] for call in visit.call_args_list}

    assert len(visited) >= 10
    assert all(url.startswith('https://crawlee.dev/docs/examples') for url in visited)


async def test_nonexistent_url_invokes_error_handler() -> None:
    crawler = PlaywrightCrawler(
        max_request_retries=4, request_provider=RequestList(['https://this-does-not-exist-22343434.com'])
    )

    error_handler = mock.AsyncMock(return_value=None)
    crawler.error_handler(error_handler)

    failed_handler = mock.AsyncMock(return_value=None)
    crawler.failed_request_handler(failed_handler)

    @crawler.router.default_handler
    async def request_handler(_context: PlaywrightCrawlingContext) -> None:
        pass

    await crawler.run()
    assert error_handler.call_count == 3
    assert failed_handler.call_count == 1
