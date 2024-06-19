from __future__ import annotations

from typing import TYPE_CHECKING, AsyncGenerator
from unittest import mock

import pytest
import respx
from httpx import Response

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler
from crawlee.storages import RequestList

if TYPE_CHECKING:
    from crawlee.beautifulsoup_crawler import BeautifulSoupCrawlingContext


@pytest.fixture()
async def server() -> AsyncGenerator[respx.MockRouter, None]:
    with respx.mock(base_url='https://test.io', assert_all_called=False) as mock:
        mock.get('/', name='index_endpoint').return_value = Response(
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

        mock.get('/asdf', name='secondary_index_endpoint').return_value = Response(
            200,
            text="""<html>
                <head>
                    <title>Hello</title>
                </head>
                <body>
                    <a href="/uiop">Link 3</a>
                    <a href="/qwer">Link 4</a>
                </body>
            </html>""",
        )

        generic_response = Response(
            200,
            text="""<html>
                <head>
                    <title>Hello</title>
                </head>
                <body>
                    Insightful content
                </body>
            </html>""",
        )

        mock.get('/hjkl').return_value = generic_response
        mock.get('/qwer').return_value = generic_response
        mock.get('/uiop').return_value = generic_response

        yield mock


async def test_basic(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler(request_provider=RequestList(['https://test.io/']))
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        links = context.soup.find_all('a')
        await handler(links)

    await crawler.run()

    assert server['index_endpoint'].called
    assert handler.called

    # The handler should find two links
    assert len(handler.call_args[0][0]) == 2


async def test_enqueue_links(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler(request_provider=RequestList(['https://test.io/']))
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links()

    await crawler.run()

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
