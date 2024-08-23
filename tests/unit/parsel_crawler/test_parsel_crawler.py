from __future__ import annotations

import sys
from typing import TYPE_CHECKING, AsyncGenerator
from unittest import mock

import pytest
import respx
from httpx import Response

from crawlee import ConcurrencySettings
from crawlee._models import BaseRequestData
from crawlee.parsel_crawler import ParselCrawler
from crawlee.storages import RequestList

if TYPE_CHECKING:
    from crawlee.parsel_crawler import ParselCrawlingContext


@pytest.fixture
async def server() -> AsyncGenerator[respx.MockRouter, None]:
    with respx.mock(base_url='https://test.io', assert_all_called=False) as mock:
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
    crawler = ParselCrawler(request_provider=RequestList(['https://test.io/']))
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        links = context.selector.css('a::attr(href)').getall()
        await handler(links)

    await crawler.run()

    assert server['index_endpoint'].called
    assert handler.called

    # The handler should find two links
    assert len(handler.call_args[0][0]) == 2


async def test_enqueue_links(server: respx.MockRouter) -> None:
    crawler = ParselCrawler(request_provider=RequestList(['https://test.io/']))
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        url = str(context.request.url)
        visit(url)
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


async def test_enqueue_links_selector(server: respx.MockRouter) -> None:
    crawler = ParselCrawler(request_provider=RequestList(['https://test.io/']))
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        url = str(context.request.url)
        visit(url)
        await context.enqueue_links(selector='a.foo', label='foo')

    with mock.patch.object(BaseRequestData, 'from_url', wraps=BaseRequestData.from_url) as from_url:
        await crawler.run()

    assert server['index_endpoint'].called
    assert server['secondary_index_endpoint'].called

    visited = {call[0][0] for call in visit.call_args_list}
    assert visited == {
        'https://test.io/',
        'https://test.io/asdf',
    }

    assert from_url.call_count == 1
    assert from_url.call_args == (('https://test.io/asdf',), {'user_data': {'label': 'foo'}})


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


async def test_handle_blocked_request(server: respx.MockRouter) -> None:
    crawler = ParselCrawler(request_provider=RequestList(['https://test.io/fdyr']), max_session_rotations=1)
    stats = await crawler.run()
    assert server['incapsula_endpoint'].called
    assert stats.requests_failed == 1


async def test_handle_blocked_status_code(server: respx.MockRouter) -> None:
    crawler = ParselCrawler(request_provider=RequestList(['https://test.io/blocked']), max_session_rotations=1)

    # Patch internal calls and run crawler
    with mock.patch.object(
        crawler._statistics,
        'record_request_processing_failure',
        wraps=crawler._statistics.record_request_processing_failure,
    ) as record_request_processing_failure, mock.patch.object(
        crawler._statistics.error_tracker, 'add', wraps=crawler._statistics.error_tracker.add
    ) as error_tracker_add:
        stats = await crawler.run()

    assert server['blocked_endpoint'].called
    assert stats.requests_failed == 1
    assert record_request_processing_failure.called
    assert error_tracker_add.called
    assert crawler._statistics.error_tracker.total == 1


def test_import_error_handled() -> None:
    # Simulate ImportError for parsel
    with mock.patch.dict('sys.modules', {'parsel': None}):
        # Invalidate ParselCrawler import
        sys.modules.pop('crawlee.parsel_crawler', None)
        sys.modules.pop('crawlee.parsel_crawler._parsel_crawler', None)

        with pytest.raises(ImportError) as import_error:
            from crawlee.parsel_crawler import ParselCrawler  # noqa: F401

    # Check if the raised ImportError contains the expected message
    assert str(import_error.value) == (
        "To import anything from this subpackage, you need to install the 'parsel' extra."
        "For example, if you use pip, run `pip install 'crawlee[parsel]'`."
    )
