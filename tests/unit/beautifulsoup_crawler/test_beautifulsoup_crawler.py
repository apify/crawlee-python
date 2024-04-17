from typing import AsyncGenerator
from unittest import mock

import pytest
import respx
from httpx import Response

from crawlee.beautifulsoup_crawler.beautifulsoup_crawler import BeautifulSoupCrawler
from crawlee.beautifulsoup_crawler.types import BeautifulSoupCrawlingContext
from crawlee.storages.request_list import RequestList


@pytest.fixture()
async def server() -> AsyncGenerator[respx.MockRouter, None]:
    with respx.mock(base_url='https://test.io', assert_all_called=False) as mock:
        mock.get('/html', name='html_endpoint').return_value = Response(
            200,
            text="""<html>
                <head>
                    <title>Hello</title>
                </head>
                <body>
                    <a href="/asdf">Link 1</a>
                    <a href="/hjkl">Link 2</a>
                </body>
            </html>""",
        )

        yield mock


async def test_basic(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler(request_provider=RequestList(['https://test.io/html']))
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        links = context.soup.find_all('a')
        await handler(links)

    await crawler.run()

    assert server['html_endpoint'].called
    assert handler.called

    # The handler should find two links
    assert len(handler.call_args[0][0]) == 2
