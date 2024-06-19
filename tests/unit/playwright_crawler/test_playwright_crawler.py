from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest import mock

from crawlee.playwright_crawler import PlaywrightCrawler

if TYPE_CHECKING:
    from crawlee.playwright_crawler import PlaywrightCrawlingContext


async def test_basic_request(httpbin: str) -> None:
    requests = [f'{httpbin}/']
    crawler = PlaywrightCrawler()
    result: dict = {}

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        assert context.page is not None
        result['request_url'] = context.request.url
        result['page_url'] = context.page.url
        result['page_title'] = await context.page.title()
        result['page_content'] = await context.page.content()

    await crawler.run(requests)

    assert result.get('request_url') == result.get('page_url') == f'{httpbin}/'
    assert 'httpbin' in result.get('page_title', '')
    assert '<html' in result.get('page_content', '')  # there is some HTML content


async def test_enqueue_links(server: Any) -> None:
    requests = ['https://test.io/']
    crawler = PlaywrightCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links()

    await crawler.run(requests)

    assert server['index_endpoint'].called
    assert server['secondary_index_endpoint'].called

    visited = {call[0][0] for call in visit.call_args_list}
    assert visited == {
        'https://test.io/',
        'https://test.io/asdf',
        'https://test.io/hjkl',
        'https://test.io/qwer',
        'https://test.io/uiop',
    }
