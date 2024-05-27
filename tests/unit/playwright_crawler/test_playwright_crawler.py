from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

from crawlee.playwright_crawler import PlaywrightCrawler
from crawlee.storages.request_list import RequestList

if TYPE_CHECKING:
    from crawlee.playwright_crawler import PlaywrightCrawlingContext


async def test_basic() -> None:
    request_provider = RequestList(['https://example.com/'])
    crawler = PlaywrightCrawler(request_provider=request_provider)
    handler = AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        assert context.page is not None
        assert context.page.url == context.request.url == 'https://example.com/'
        assert await context.page.title() == 'Example Domain'
        assert (await context.page.content()).split('\n')[0] == '<!DOCTYPE html><html><head>'
        await handler()

    await crawler.run()
    assert handler.called
