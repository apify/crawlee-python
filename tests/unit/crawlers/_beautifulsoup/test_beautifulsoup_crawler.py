from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from crawlee import ConcurrencySettings, Glob, HttpHeaders, RequestTransformAction, SkippedReason
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
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

    # The handler should find two links
    assert len(handler.call_args[0][0]) == 2


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
        concurrency_settings=ConcurrencySettings(max_concurrency=1), max_requests_per_crawl=3, http_client=http_client
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
