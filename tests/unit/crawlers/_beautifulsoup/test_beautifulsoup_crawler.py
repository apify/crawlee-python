from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest
import respx
from httpx import Response

from crawlee import ConcurrencySettings, HttpHeaders, RequestTransformAction
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee._request import RequestOptions


@pytest.fixture
async def server() -> AsyncGenerator[respx.MockRouter, None]:
    with respx.mock(base_url='https://test.io', assert_all_called=False) as mock:
        mock.get('https://www.test.io/').return_value = Response(302, headers={'Location': 'https://test.io/'})

        mock.get('/', name='index_endpoint').return_value = Response(
            200,
            text="""<html>
                <head>
                    <title>Hello</title>
                </head>
                <body>
                    <a href="/asdf" class="foo">Link 1</a>
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

        mock.get('/fdyr', name='incapsula_endpoint').return_value = Response(
            200,
            text="""<html>
                <head>
                    <title>Hello</title>
                </head>
                <body>
                    <iframe src=Test_Incapsula_Resource>
                    </iframe>
                </body>
            </html>""",
        )

        mock.get('/hjkl').return_value = generic_response
        mock.get('/qwer').return_value = generic_response
        mock.get('/uiop').return_value = generic_response

        yield mock


async def test_basic(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler()
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        links = context.soup.find_all('a')
        await handler(links)

    await crawler.run(['https://test.io/'])

    assert server['index_endpoint'].called
    assert handler.called

    # The handler should find two links
    assert len(handler.call_args[0][0]) == 2


async def test_enqueue_links(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links()

    await crawler.run(['https://www.test.io/'])

    assert server['index_endpoint'].called
    assert server['secondary_index_endpoint'].called

    visited = {call[0][0] for call in visit.call_args_list}

    assert visited == {
        'https://www.test.io/',
        'https://test.io/asdf',
        'https://test.io/hjkl',
        'https://test.io/qwer',
        'https://test.io/uiop',
    }


async def test_enqueue_links_selector(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links(selector='a.foo')

    await crawler.run(['https://test.io/'])

    assert server['index_endpoint'].called
    assert server['secondary_index_endpoint'].called

    visited = {call[0][0] for call in visit.call_args_list}
    assert visited == {'https://test.io/', 'https://test.io/asdf'}


async def test_enqueue_links_with_max_crawl(server: respx.MockRouter) -> None:
    start_urls = ['https://test.io/']
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = BeautifulSoupCrawler(
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
        max_requests_per_crawl=3,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        await context.enqueue_links()
        processed_urls.append(context.request.url)

    stats = await crawler.run(start_urls)

    # Verify that only 3 out of the possible 5 requests were made
    assert server['index_endpoint'].called
    assert len(processed_urls) == 3
    assert stats.requests_total == 3
    assert stats.requests_finished == 3


async def test_enqueue_links_with_transform_request_function(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler()
    visit = mock.Mock()
    headers = []

    def test_transform_request_function(
        request_options: RequestOptions,
    ) -> RequestOptions | RequestTransformAction:
        if 'uiop' in request_options['url']:
            return 'skip'

        request_options['headers'] = HttpHeaders({'transform-header': 'my-header'})
        return request_options

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        headers.append(context.request.headers)

        await context.enqueue_links(transform_request_function=test_transform_request_function)

    await crawler.run(['https://test.io/'])

    assert server['index_endpoint'].called
    assert server['secondary_index_endpoint'].called

    visited = {call[0][0] for call in visit.call_args_list}

    # url https://test.io/uiop should not be visited
    assert visited == {'https://test.io/', 'https://test.io/asdf', 'https://test.io/hjkl', 'https://test.io/qwer'}

    # # all urls added to `enqueue_links` must have a custom header
    assert headers[1]['transform-header'] == 'my-header'
    assert headers[2]['transform-header'] == 'my-header'
    assert headers[3]['transform-header'] == 'my-header'


async def test_handle_blocked_request(server: respx.MockRouter) -> None:
    crawler = BeautifulSoupCrawler(max_session_rotations=1)
    stats = await crawler.run(['https://test.io/fdyr'])
    assert server['incapsula_endpoint'].called
    assert stats.requests_failed == 1


def test_default_logger() -> None:
    assert BeautifulSoupCrawler().log.name == 'BeautifulSoupCrawler'
