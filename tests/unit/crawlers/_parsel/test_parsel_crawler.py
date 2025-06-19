from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from crawlee import ConcurrencySettings, Glob, HttpHeaders, Request, RequestTransformAction, SkippedReason
from crawlee.crawlers import ParselCrawler

if TYPE_CHECKING:
    from yarl import URL

    from crawlee._request import RequestOptions
    from crawlee.crawlers import ParselCrawlingContext
    from crawlee.http_clients._base import HttpClient


async def test_basic(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client)
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        links = context.selector.css('a::attr(href)').getall()
        await handler(links)

    await crawler.run([str(server_url / 'start_enqueue')])

    assert handler.called

    # The handler should find two links
    assert len(handler.call_args[0][0]) == 2


async def test_enqueue_links(redirect_server_url: URL, server_url: URL, http_client: HttpClient) -> None:
    redirect_target = str(server_url / 'start_enqueue')
    redirect_url = str(redirect_server_url.with_path('redirect').with_query(url=redirect_target))
    requests = [redirect_url]

    crawler = ParselCrawler(http_client=http_client)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        url = str(context.request.url)
        visit(url)
        await context.enqueue_links()

    await crawler.run(requests)

    first_visited = visit.call_args_list[0][0][0]
    visited = {call[0][0] for call in visit.call_args_list}

    assert first_visited == redirect_url
    assert visited == {
        redirect_url,
        str(server_url / 'sub_index'),
        str(server_url / 'page_1'),
        str(server_url / 'page_2'),
        str(server_url / 'page_3'),
    }


async def test_enqueue_links_with_incompatible_kwargs_raises_error(server_url: URL) -> None:
    """Call `enqueue_links` with arguments that can't be used together."""
    crawler = ParselCrawler(max_request_retries=1)
    exceptions = []

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        try:
            await context.enqueue_links(requests=[Request.from_url(str(server_url / 'start_enqueue'))], selector='a')  # type:ignore[call-overload]  # Testing runtime enforcement of the overloads.
        except Exception as e:
            exceptions.append(e)

    await crawler.run([str(server_url)])

    assert len(exceptions) == 1
    assert type(exceptions[0]) is ValueError


async def test_enqueue_links_selector(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links(selector='a.foo')

    await crawler.run([str(server_url / 'start_enqueue')])

    visited = {call[0][0] for call in visit.call_args_list}
    assert visited == {str(server_url / 'start_enqueue'), str(server_url / 'sub_index')}


async def test_enqueue_links_with_max_crawl(server_url: URL, http_client: HttpClient) -> None:
    start_urls = [str(server_url / 'start_enqueue')]
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = ParselCrawler(
        concurrency_settings=ConcurrencySettings(max_concurrency=1), max_requests_per_crawl=3, http_client=http_client
    )

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        await context.enqueue_links()
        processed_urls.append(context.request.url)

    stats = await crawler.run(start_urls)

    # Verify that only 3 out of the possible 5 requests were made
    assert len(processed_urls) == 3
    assert stats.requests_total == 3
    assert stats.requests_finished == 3


async def test_enqueue_links_with_transform_request_function(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client)
    visit = mock.Mock()
    headers = []

    def test_transform_request_function(
        request_options: RequestOptions,
    ) -> RequestOptions | RequestTransformAction:
        if 'page_3' in request_options['url']:
            return 'skip'

        request_options['headers'] = HttpHeaders({'transform-header': 'my-header'})
        return request_options

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        visit(context.request.url)
        headers.append(context.request.headers)
        await context.enqueue_links(transform_request_function=test_transform_request_function, label='test')

    await crawler.run([str(server_url / 'start_enqueue')])

    visited = {call[0][0] for call in visit.call_args_list}

    # url /page_3 should not be visited
    assert visited == {
        str(server_url / 'start_enqueue'),
        str(server_url / 'sub_index'),
        str(server_url / 'page_1'),
        str(server_url / 'page_2'),
    }

    # # all urls added to `enqueue_links` must have a custom header
    assert headers[1]['transform-header'] == 'my-header'
    assert headers[2]['transform-header'] == 'my-header'
    assert headers[3]['transform-header'] == 'my-header'


async def test_handle_blocked_request(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(max_session_rotations=1, http_client=http_client)

    stats = await crawler.run([str(server_url / 'incapsula')])
    assert stats.requests_failed == 1


async def test_handle_blocked_status_code(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(max_session_rotations=1, http_client=http_client)

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
        stats = await crawler.run([str(server_url / 'status/403')])

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


async def test_json(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client)
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        result = context.selector.jmespath('hello').getall()
        await handler(result)

    await crawler.run([str(server_url / 'json')])

    assert handler.called

    assert handler.call_args[0][0] == ['world']


async def test_xml(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client)
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        result = context.selector.css('hello').getall()
        await handler(result)

    await crawler.run([str(server_url / 'xml')])

    assert handler.called

    assert handler.call_args[0][0] == ['<hello>world</hello>']


def test_default_logger() -> None:
    assert ParselCrawler().log.name == 'ParselCrawler'


async def test_respect_robots_txt(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client, respect_robots_txt_file=True)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links()

    await crawler.run([str(server_url / 'start_enqueue')])
    visited = {call[0][0] for call in visit.call_args_list}

    assert visited == {
        str(server_url / 'start_enqueue'),
        str(server_url / 'sub_index'),
    }


async def test_on_skipped_request(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client, respect_robots_txt_file=True)
    skip = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        await context.enqueue_links()

    @crawler.on_skipped_request
    async def skipped_hook(url: str, _reason: SkippedReason) -> None:
        skip(url)

    await crawler.run([str(server_url / 'start_enqueue')])

    skipped = {call[0][0] for call in skip.call_args_list}

    assert skipped == {
        str(server_url / 'page_1'),
        str(server_url / 'page_2'),
        str(server_url / 'page_3'),
    }


async def test_extract_links(server_url: URL, http_client: HttpClient) -> None:
    crawler = ParselCrawler(http_client=http_client)
    extracted_links: list[str] = []

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        links = await context.extract_links(exclude=[Glob(f'{server_url}sub_index')])
        extracted_links.extend(request.url for request in links)

    await crawler.run([str(server_url / 'start_enqueue')])

    assert len(extracted_links) == 1
    assert extracted_links[0] == str(server_url / 'page_1')
