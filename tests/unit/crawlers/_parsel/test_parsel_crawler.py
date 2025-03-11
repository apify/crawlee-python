from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import respx
from httpx import Response

from crawlee import ConcurrencySettings, HttpHeaders, RequestTransformAction
from crawlee.crawlers import ParselCrawler

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee._request import RequestOptions
    from crawlee.crawlers import ParselCrawlingContext


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

        mock.get('/blocked', name='blocked_endpoint').return_value = Response(
            403,
            text="""<html>
                <head>
                    <title>Blocked</title>
                </head>
                <body>
                    <h3>Forbidden</h3>
                </body>
            </html>""",
        )

        mock.get('/json', name='json_endpoint').return_value = Response(
            200,
            text="""{
                "hello": "world"
            }""",
        )

        mock.get('/xml', name='xml_endpoint').return_value = Response(
            200,
            text="""
                <?xml version="1.0"?>
                <hello>world</hello>
            """,
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
    crawler = ParselCrawler()
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        links = context.selector.css('a::attr(href)').getall()
        await handler(links)

    await crawler.run(['https://test.io/'])

    assert server['index_endpoint'].called
    assert handler.called

    # The handler should find two links
    assert len(handler.call_args[0][0]) == 2


async def test_enqueue_links(server: respx.MockRouter) -> None:
    crawler = ParselCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        url = str(context.request.url)
        visit(url)
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
    crawler = ParselCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
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
    crawler = ParselCrawler(
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
        max_requests_per_crawl=3,
    )

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        await context.enqueue_links()
        processed_urls.append(context.request.url)

    stats = await crawler.run(start_urls)

    # Verify that only 3 out of the possible 5 requests were made
    assert server['index_endpoint'].called
    assert len(processed_urls) == 3
    assert stats.requests_total == 3
    assert stats.requests_finished == 3


async def test_enqueue_links_with_transform_request_function(server: respx.MockRouter) -> None:
    crawler = ParselCrawler()
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
    async def request_handler(context: ParselCrawlingContext) -> None:
        visit(context.request.url)
        headers.append(context.request.headers)
        await context.enqueue_links(transform_request_function=test_transform_request_function, label='test')

    await crawler.run(['https://test.io/'])

    assert server['index_endpoint'].called
    assert server['secondary_index_endpoint'].called

    visited = {call[0][0] for call in visit.call_args_list}

    # url https://test.io/uiop should not be visited
    assert visited == {'https://test.io/', 'https://test.io/asdf', 'https://test.io/hjkl', 'https://test.io/qwer'}

    # all urls added to `enqueue_links` must have a custom header
    assert headers[1]['transform-header'] == 'my-header'
    assert headers[2]['transform-header'] == 'my-header'
    assert headers[3]['transform-header'] == 'my-header'


async def test_handle_blocked_request(server: respx.MockRouter) -> None:
    crawler = ParselCrawler(max_session_rotations=1)

    stats = await crawler.run(['https://test.io/fdyr'])
    assert server['incapsula_endpoint'].called
    assert stats.requests_failed == 1


async def test_handle_blocked_status_code(server: respx.MockRouter) -> None:
    crawler = ParselCrawler(max_session_rotations=1)

    # Patch internal calls and run crawler
    with (
        mock.patch.object(
            crawler._statistics,
            'record_request_processing_failure',
            wraps=crawler._statistics.record_request_processing_failure,
        ) as record_request_processing_failure,
        mock.patch.object(
            crawler._statistics.error_tracker, 'add', wraps=crawler._statistics.error_tracker.add
        ) as error_tracker_add,
    ):
        stats = await crawler.run(['https://test.io/blocked'])

    assert server['blocked_endpoint'].called
    assert stats.requests_failed == 1
    assert record_request_processing_failure.called
    assert error_tracker_add.called
    assert crawler._statistics.error_tracker.total == 1


# TODO: Remove the skip mark when the test is fixed:
# https://github.com/apify/crawlee-python/issues/838
@pytest.mark.skip(reason='The test does not work with `crawlee._utils.try_import.ImportWrapper`.')
def test_import_error_handled() -> None:
    # Simulate ImportError for parsel
    with mock.patch.dict('sys.modules', {'parsel': None}):
        # Invalidate ParselCrawler import
        sys.modules.pop('crawlee.crawlers', None)
        sys.modules.pop('crawlee.crawlers._parsel', None)

        with pytest.raises(ImportError) as import_error:
            from crawlee.crawlers import ParselCrawler  # noqa: F401

    # Check if the raised ImportError contains the expected message
    assert str(import_error.value) == (
        "To import this, you need to install the 'parsel' extra."
        "For example, if you use pip, run `pip install 'crawlee[parsel]'`."
    )


async def test_json(server: respx.MockRouter) -> None:
    crawler = ParselCrawler()
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        result = context.selector.jmespath('hello').getall()
        await handler(result)

    await crawler.run(['https://test.io/json'])

    assert server['json_endpoint'].called
    assert handler.called

    assert handler.call_args[0][0] == ['world']


async def test_xml(server: respx.MockRouter) -> None:
    crawler = ParselCrawler()
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        result = context.selector.css('hello').getall()
        await handler(result)

    await crawler.run(['https://test.io/xml'])

    assert server['xml_endpoint'].called
    assert handler.called

    assert handler.call_args[0][0] == ['<hello>world</hello>']


def test_default_logger() -> None:
    assert ParselCrawler().log.name == 'ParselCrawler'
