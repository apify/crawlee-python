from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from crawlee import ConcurrencySettings, Glob, HttpHeaders, Request, RequestTransformAction, SkippedReason
from crawlee.crawlers import BasicCrawlingContext, BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import RequestQueue

if TYPE_CHECKING:
    from yarl import URL

    from crawlee._request import RequestOptions
    from crawlee.http_clients._base import HttpClient


async def test_basic(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client)
    handler = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        links = context.soup.find_all('a')
        await handler(links)

    await crawler.run([str(server_url / 'start_enqueue')])

    assert handler.called

    # The handler should find three links
    assert len(handler.call_args[0][0]) == 3


async def test_enqueue_links(redirect_server_url: URL, server_url: URL, http_client: HttpClient) -> None:
    redirect_target = str(server_url / 'start_enqueue')
    redirect_url = str(redirect_server_url.with_path('redirect').with_query(url=redirect_target))
    requests = [redirect_url]

    crawler = BeautifulSoupCrawler(http_client=http_client)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
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
        str(server_url / 'page_4'),
        str(server_url / 'base_page'),
        str(server_url / 'base_subpath/page_5'),
    }


async def test_enqueue_links_selector(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links(selector='a.foo')

    await crawler.run([str(server_url / 'start_enqueue')])

    visited = {call[0][0] for call in visit.call_args_list}
    assert visited == {str(server_url / 'start_enqueue'), str(server_url / 'sub_index')}


async def test_enqueue_links_with_max_crawl(server_url: URL, http_client: HttpClient) -> None:
    start_urls = [str(server_url / 'start_enqueue')]
    processed_urls = []

    # Set max_concurrency to 1 to ensure testing max_requests_per_crawl accurately
    crawler = BeautifulSoupCrawler(
        concurrency_settings=ConcurrencySettings(desired_concurrency=1, max_concurrency=1),
        max_requests_per_crawl=3,
        http_client=http_client,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        await context.enqueue_links()
        processed_urls.append(context.request.url)

    stats = await crawler.run(start_urls)

    # Verify that only 3 out of the possible 5 requests were made
    assert len(processed_urls) == 3
    assert stats.requests_total == 3
    assert stats.requests_finished == 3


async def test_enqueue_links_with_transform_request_function(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client)
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
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        headers.append(context.request.headers)

        await context.enqueue_links(transform_request_function=test_transform_request_function)

    await crawler.run([str(server_url / 'start_enqueue')])

    visited = {call[0][0] for call in visit.call_args_list}

    # url /page_3 should not be visited
    assert visited == {
        str(server_url / 'start_enqueue'),
        str(server_url / 'sub_index'),
        str(server_url / 'page_1'),
        str(server_url / 'page_2'),
        str(server_url / 'base_page'),
        str(server_url / 'page_4'),
        str(server_url / 'base_subpath/page_5'),
    }

    # # all urls added to `enqueue_links` must have a custom header
    assert headers[1]['transform-header'] == 'my-header'
    assert headers[2]['transform-header'] == 'my-header'
    assert headers[3]['transform-header'] == 'my-header'


async def test_handle_blocked_request(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(max_session_rotations=1, http_client=http_client)
    stats = await crawler.run([str(server_url / 'incapsula')])
    assert stats.requests_failed == 1


def test_default_logger() -> None:
    assert BeautifulSoupCrawler().log.name == 'BeautifulSoupCrawler'


async def test_respect_robots_txt(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client, respect_robots_txt_file=True)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links()

    await crawler.run([str(server_url / 'start_enqueue')])
    visited = {call[0][0] for call in visit.call_args_list}

    assert visited == {
        str(server_url / 'start_enqueue'),
        str(server_url / 'sub_index'),
        str(server_url / 'base_page'),
        str(server_url / 'base_subpath/page_5'),
    }


async def test_respect_robots_txt_with_problematic_links(server_url: URL, http_client: HttpClient) -> None:
    """Test checks the crawler behavior with links that may cause problems when attempting to retrieve robots.txt."""
    visit = mock.Mock()
    fail = mock.Mock()
    crawler = BeautifulSoupCrawler(
        http_client=http_client,
        respect_robots_txt_file=True,
        max_request_retries=0,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links(strategy='all')

    @crawler.failed_request_handler
    async def error_handler(context: BasicCrawlingContext, _error: Exception) -> None:
        fail(context.request.url)

    await crawler.run([str(server_url / 'problematic_links')])

    visited = {call[0][0] for call in visit.call_args_list}
    failed = {call[0][0] for call in fail.call_args_list}

    # Email must be skipped
    # https://avatars.githubusercontent.com/apify does not get robots.txt, but is correct for the crawler.
    assert visited == {str(server_url / 'problematic_links'), 'https://avatars.githubusercontent.com/apify'}

    # The budplaceholder.com does not exist.
    assert failed == {
        'https://budplaceholder.com/',
    }


async def test_on_skipped_request(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client, respect_robots_txt_file=True)
    skip = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
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
        str(server_url / 'page_4'),
    }


async def test_extract_links(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client)
    extracted_links: list[str] = []

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        links = await context.extract_links(exclude=[Glob(f'{server_url}sub_index')])
        extracted_links.extend(request.url for request in links)

    await crawler.run([str(server_url / 'start_enqueue')])

    assert len(extracted_links) == 1
    assert extracted_links[0] == str(server_url / 'page_1')


@pytest.mark.parametrize(
    ('queue_name', 'queue_alias', 'by_id'),
    [
        pytest.param('named-queue', None, False, id='with rq_name'),
        pytest.param(None, 'alias-queue', False, id='with rq_alias'),
        pytest.param('id-queue', None, True, id='with rq_id'),
    ],
)
async def test_enqueue_links_with_rq_param(
    server_url: URL, http_client: HttpClient, queue_name: str | None, queue_alias: str | None, *, by_id: bool
) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client)
    rq = await RequestQueue.open(name=queue_name, alias=queue_alias)
    if by_id:
        queue_name = None
        queue_id = rq.id
    else:
        queue_id = None
    visit_urls: set[str] = set()

    @crawler.router.default_handler
    async def handler(context: BeautifulSoupCrawlingContext) -> None:
        visit_urls.add(context.request.url)
        await context.enqueue_links(rq_id=queue_id, rq_name=queue_name, rq_alias=queue_alias)

    await crawler.run([str(server_url / 'start_enqueue')])

    requests_from_queue: list[str] = []
    while request := await rq.fetch_next_request():
        requests_from_queue.append(request.url)

    assert set(requests_from_queue) == {str(server_url / 'page_1'), str(server_url / 'sub_index')}
    assert visit_urls == {str(server_url / 'start_enqueue')}

    await rq.drop()


@pytest.mark.parametrize(
    ('queue_name', 'queue_alias', 'by_id'),
    [
        pytest.param('named-queue', None, False, id='with rq_name'),
        pytest.param(None, 'alias-queue', False, id='with rq_alias'),
        pytest.param('id-queue', None, True, id='with rq_id'),
    ],
)
async def test_enqueue_links_requests_with_rq_param(
    server_url: URL, http_client: HttpClient, queue_name: str | None, queue_alias: str | None, *, by_id: bool
) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client)
    rq = await RequestQueue.open(name=queue_name, alias=queue_alias)
    if by_id:
        queue_name = None
        queue_id = rq.id
    else:
        queue_id = None
    visit_urls: set[str] = set()

    check_requests: list[str] = [
        'https://a.placeholder.com',
        'https://b.placeholder.com',
        'https://c.placeholder.com',
    ]

    @crawler.router.default_handler
    async def handler(context: BeautifulSoupCrawlingContext) -> None:
        visit_urls.add(context.request.url)
        await context.enqueue_links(
            requests=check_requests, rq_name=queue_name, rq_alias=queue_alias, rq_id=queue_id, strategy='all'
        )

    await crawler.run([str(server_url / 'start_enqueue')])

    requests_from_queue: list[str] = []
    while request := await rq.fetch_next_request():
        requests_from_queue.append(request.url)

    assert set(requests_from_queue) == set(check_requests)
    assert visit_urls == {str(server_url / 'start_enqueue')}

    await rq.drop()


@pytest.mark.parametrize(
    ('queue_id', 'queue_name', 'queue_alias'),
    [
        pytest.param('named-queue', 'alias-queue', None, id='rq_name and rq_alias'),
        pytest.param('named-queue', None, 'id-queue', id='rq_name and rq_id'),
        pytest.param(None, 'alias-queue', 'id-queue', id='rq_alias and rq_id'),
        pytest.param('named-queue', 'alias-queue', 'id-queue', id='rq_name and rq_alias and rq_id'),
    ],
)
async def test_enqueue_links_error_with_multi_params(
    server_url: URL, http_client: HttpClient, queue_id: str | None, queue_name: str | None, queue_alias: str | None
) -> None:
    crawler = BeautifulSoupCrawler(http_client=http_client)

    @crawler.router.default_handler
    async def handler(context: BeautifulSoupCrawlingContext) -> None:
        with pytest.raises(ValueError, match='Cannot use both `rq_name` and `rq_alias`'):
            await context.enqueue_links(rq_id=queue_id, rq_name=queue_name, rq_alias=queue_alias)

    await crawler.run([str(server_url / 'start_enqueue')])


async def test_navigation_timeout_on_slow_request(server_url: URL, http_client: HttpClient) -> None:
    """Test that navigation_timeout causes TimeoutError on slow HTTP requests."""
    crawler = BeautifulSoupCrawler(
        http_client=http_client,
        navigation_timeout=timedelta(seconds=1),
        max_request_retries=0,
    )

    failed_request_handler = mock.AsyncMock()
    crawler.failed_request_handler(failed_request_handler)

    request_handler = mock.AsyncMock()
    crawler.router.default_handler(request_handler)

    # Request endpoint that delays 5 seconds - should timeout at 1 second
    await crawler.run([str(server_url.with_path('/slow').with_query(delay=5))])

    assert failed_request_handler.call_count == 1
    assert isinstance(failed_request_handler.call_args[0][1], asyncio.TimeoutError)


async def test_navigation_timeout_applies_to_hooks(server_url: URL) -> None:
    crawler = BeautifulSoupCrawler(
        navigation_timeout=timedelta(seconds=1),
        max_request_retries=0,
    )

    request_handler = mock.AsyncMock()
    crawler.router.default_handler(request_handler)
    crawler.pre_navigation_hook(lambda _: asyncio.sleep(1))

    # Pre-navigation hook takes 1 second (exceeds navigation timeout), so the URL will not be handled
    result = await crawler.run([str(server_url)])

    assert result.requests_failed == 1
    assert result.requests_finished == 0
    assert request_handler.call_count == 0


async def test_slow_navigation_does_not_count_toward_handler_timeout(server_url: URL, http_client: HttpClient) -> None:
    crawler = BeautifulSoupCrawler(
        http_client=http_client,
        request_handler_timeout=timedelta(seconds=0.5),
        max_request_retries=0,
    )

    request_handler = mock.AsyncMock()
    crawler.router.default_handler(request_handler)

    # Navigation takes 1 second (exceeds handler timeout), but should still succeed
    result = await crawler.run([str(server_url.with_path('/slow').with_query(delay=1))])

    assert result.requests_failed == 0
    assert result.requests_finished == 1
    assert request_handler.call_count == 1


async def test_enqueue_strategy_after_redirect(server_url: URL, redirect_server_url: URL) -> None:
    crawler = BeautifulSoupCrawler()

    handler_calls = mock.AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        await handler_calls(context.request.url)

        target_url = str(server_url.with_path('redirect').with_query(url=str(redirect_server_url)))

        await context.enqueue_links(requests=[Request.from_url(target_url)], strategy='same-origin')

    await crawler.run([str(server_url)])

    assert handler_calls.called
    assert handler_calls.call_count == 1
