from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

from crawlee.playwright_crawler import PlaywrightCrawler
from crawlee.storages.request_list import RequestList

if TYPE_CHECKING:
    from crawlee.playwright_crawler import PlaywrightCrawlingContext


async def test_basic_request() -> None:
    request_provider = RequestList(['https://httpbin.org/'])
    crawler = PlaywrightCrawler(request_provider=request_provider)
    handler = AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        assert context.page is not None
        assert context.page.url == context.request.url == 'https://httpbin.org/'
        assert 'httpbin' in await context.page.title()
        assert '<html><head>' in await context.page.content()  # there is some HTML content
        await handler()

    await crawler.run()
    assert handler.called
