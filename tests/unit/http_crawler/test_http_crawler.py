from __future__ import annotations

from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable
from unittest.mock import AsyncMock

import pytest
import respx
from httpx import Response

from crawlee.http_crawler.http_crawler import HttpCrawler
from crawlee.storages import RequestList

if TYPE_CHECKING:
    from crawlee.http_crawler.types import HttpCrawlingContext


@pytest.fixture()
async def mock_request_handler() -> Callable[[HttpCrawlingContext], Awaitable[None]] | AsyncMock:
    return AsyncMock()


@pytest.fixture()
async def crawler(mock_request_handler: Callable[[HttpCrawlingContext], Awaitable[None]]) -> HttpCrawler:
    return HttpCrawler(router=mock_request_handler, request_provider=RequestList())


@pytest.fixture()
async def server() -> AsyncGenerator[respx.MockRouter, None]:
    with respx.mock(base_url='https://test.io', assert_all_called=False) as mock:
        mock.get('/html', name='html_endpoint').return_value = Response(
            200,
            text="""<html>
                <head>
                    <title>Hello</title>
                </head>
                <body>Hello world</body>
            </html>""",
        )

        mock.get('/redirect', name='redirect_endpoint').return_value = Response(
            301, headers={'Location': 'https://test.io/html'}
        )

        mock.get('/404', name='404_endpoint').return_value = Response(
            404,
            text="""<html>
                <head>
                    <title>Not found</title>
                </head>
            </html>""",
        )

        mock.get('/500', name='500_endpoint').return_value = Response(
            500,
            text="""<html>
                <head>
                    <title>Internal server error</title>
                </head>
            </html>""",
        )

        yield mock


async def test_fetches_html(crawler: HttpCrawler, mock_request_handler: AsyncMock, server: respx.MockRouter) -> None:
    await crawler.add_requests(['https://test.io/html'])
    await crawler.run()

    assert server['html_endpoint'].called

    mock_request_handler.assert_called_once()
    assert mock_request_handler.call_args[0][0].request.url == 'https://test.io/html'


async def test_handles_redirects(
    crawler: HttpCrawler, mock_request_handler: AsyncMock, server: respx.MockRouter
) -> None:
    await crawler.add_requests(['https://test.io/redirect'])
    await crawler.run()

    mock_request_handler.assert_called_once()
    assert mock_request_handler.call_args[0][0].request.loaded_url == 'https://test.io/html'

    assert server['redirect_endpoint'].called
    assert server['html_endpoint'].called


async def test_handles_client_errors(
    crawler: HttpCrawler, mock_request_handler: AsyncMock, server: respx.MockRouter
) -> None:
    await crawler.add_requests(['https://test.io/404'])
    await crawler.run()

    mock_request_handler.assert_called_once()
    assert mock_request_handler.call_args[0][0].request.loaded_url == 'https://test.io/404'
    assert server['404_endpoint'].called


async def test_handles_server_error(
    crawler: HttpCrawler, mock_request_handler: AsyncMock, server: respx.MockRouter
) -> None:
    await crawler.add_requests(['https://test.io/500'])
    await crawler.run()

    mock_request_handler.assert_not_called()
    assert server['500_endpoint'].called
