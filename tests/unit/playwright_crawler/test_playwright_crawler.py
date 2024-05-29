from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.playwright_crawler import PlaywrightCrawler
from crawlee.storages.request_list import RequestList

if TYPE_CHECKING:
    from crawlee.playwright_crawler import PlaywrightCrawlingContext


async def test_basic_request() -> None:
    request_provider = RequestList(['https://httpbin.org/'])
    crawler = PlaywrightCrawler(request_provider=request_provider)
    result: dict = {}

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        assert context.page is not None
        result['request_url'] = context.request.url
        result['page_url'] = context.page.url
        result['page_title'] = await context.page.title()
        result['page_content'] = await context.page.content()

    await crawler.run()

    assert result.get('request_url') == result.get('page_url') == 'https://httpbin.org/'
    assert 'httpbin' in result.get('page_title', '')
    assert '<html' in result.get('page_content', '')  # there is some HTML content
