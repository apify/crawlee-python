from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlencode

import pytest

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import HttpCrawler
from crawlee.sessions import SessionPool
from crawlee.statistics import Statistics
from tests.unit.server_endpoints import HELLO_WORLD

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from yarl import URL

    from crawlee._types import BasicCrawlingContext
    from crawlee.crawlers import HttpCrawlingContext
    from crawlee.http_clients._base import HttpClient

# Payload, e.g. data for a form submission.
PAYLOAD = {
    'custname': 'John Doe',
    'custtel': '1234567890',
    'custemail': 'johndoe@example.com',
    'size': 'large',
    'topping': '["bacon", "cheese", "mushroom"]',
    'delivery': '13:00',
    'comments': 'Please ring the doorbell upon arrival.',
}


@pytest.fixture
async def mock_request_handler() -> Callable[[HttpCrawlingContext], Awaitable[None]] | AsyncMock:
    return AsyncMock()


@pytest.fixture
async def crawler(
    http_client: HttpClient, mock_request_handler: Callable[[HttpCrawlingContext], Awaitable[None]]
) -> HttpCrawler:
    return HttpCrawler(http_client=http_client, request_handler=mock_request_handler)


@pytest.fixture
async def crawler_without_retries(
    mock_request_handler: Callable[[HttpCrawlingContext], Awaitable[None]],
) -> HttpCrawler:
    return HttpCrawler(
        request_handler=mock_request_handler,
        retry_on_blocked=False,
        max_request_retries=0,
    )


async def test_fetches_html(
    crawler: HttpCrawler,
    mock_request_handler: AsyncMock,
    server_url: URL,
) -> None:
    await crawler.add_requests([str(server_url)])
    await crawler.run()

    mock_request_handler.assert_called_once()
    assert mock_request_handler.call_args[0][0].request.url == str(server_url)


async def test_handles_redirects(crawler: HttpCrawler, mock_request_handler: AsyncMock, server_url: URL) -> None:
    redirect_target = str(server_url)
    redirect_url = str(server_url.with_path('redirect').with_query(url=redirect_target))
    await crawler.add_requests([redirect_url])
    await crawler.run()

    mock_request_handler.assert_called_once()
    assert mock_request_handler.call_args[0][0].request.loaded_url == redirect_target
    assert mock_request_handler.call_args[0][0].request.url == redirect_url


@pytest.mark.parametrize(
    ('additional_http_error_status_codes', 'ignore_http_error_status_codes', 'expected_number_error'),
    [
        # error without retry for all 4xx statuses
        pytest.param([], [], 1, id='default_behavior'),
        # make retry for codes in `additional_http_error_status_codes` list
        pytest.param([402], [], 3, id='additional_status_codes'),
        # take as successful status codes from the `ignore_http_error_status_codes` list
        pytest.param([], [402], 0, id='ignore_error_status_codes'),
        # check precedence for `additional_http_error_status_codes`
        pytest.param([402], [402], 3, id='additional_and_ignore'),
    ],
)
async def test_handles_client_errors(
    additional_http_error_status_codes: list[int],
    ignore_http_error_status_codes: list[int],
    expected_number_error: int,
    mock_request_handler: AsyncMock,
    server_url: URL,
) -> None:
    crawler = HttpCrawler(
        request_handler=mock_request_handler,
        additional_http_error_status_codes=additional_http_error_status_codes,
        ignore_http_error_status_codes=ignore_http_error_status_codes,
        max_request_retries=3,
    )

    await crawler.add_requests([str(server_url / 'status/402')])
    await crawler.run()

    assert crawler.statistics.error_tracker.total == expected_number_error

    # Request handler should not be called for error status codes.
    if expected_number_error:
        mock_request_handler.assert_not_called()
    else:
        mock_request_handler.assert_called()


@pytest.mark.parametrize(
    ('ignore_http_error_status_codes', 'use_session_pool', 'expected_session_rotate', 'expected_number_error'),
    [
        # change session and retry for no block 4xx statuses
        pytest.param([], True, 4, 1, id='default_behavior'),
        # error without retry for all 4xx statuses
        pytest.param([], False, 0, 1, id='default_behavior_without_session_pool'),
        # take as successful status codes from the `ignore_http_error_status_codes` list with Sessoion Pool
        pytest.param([403], True, 0, 0, id='ignore_error_status_codes'),
        # take as successful status codes from the `ignore_http_error_status_codes` list without Sessoion Pool
        pytest.param([403], False, 0, 0, id='ignore_error_status_codes_without_session_pool'),
    ],
)
async def test_handles_session_block_errors(
    *,
    ignore_http_error_status_codes: list[int],
    use_session_pool: bool,
    expected_session_rotate: int,
    expected_number_error: int,
    mock_request_handler: AsyncMock,
    server_url: URL,
) -> None:
    crawler = HttpCrawler(
        request_handler=mock_request_handler,
        ignore_http_error_status_codes=ignore_http_error_status_codes,
        max_request_retries=3,
        max_session_rotations=5,
        use_session_pool=use_session_pool,
    )

    await crawler.add_requests([str(server_url / 'status/403')])
    await crawler.run()

    assert crawler.statistics.error_tracker.total == expected_number_error
    assert crawler.statistics.error_tracker_retry.total == expected_session_rotate

    # Request handler should not be called for error status codes.
    if expected_number_error:
        mock_request_handler.assert_not_called()
    else:
        mock_request_handler.assert_called()


async def test_handles_server_error(crawler: HttpCrawler, mock_request_handler: AsyncMock, server_url: URL) -> None:
    await crawler.add_requests([str(server_url / 'status/500')])
    await crawler.run()

    mock_request_handler.assert_not_called()


async def test_stores_cookies(http_client: HttpClient, server_url: URL) -> None:
    visit = Mock()
    track_session_usage = Mock()

    async with SessionPool(max_pool_size=1) as session_pool:
        crawler = HttpCrawler(
            # /cookies/set might redirect us to a page that we can't access - no problem, we only care about cookies
            ignore_http_error_status_codes=[401],
            session_pool=session_pool,
            http_client=http_client,
        )

        @crawler.router.default_handler
        async def handler(context: HttpCrawlingContext) -> None:
            visit(context.request.url)
            track_session_usage(context.session.id if context.session else None)

        await crawler.run(
            [
                str(server_url.with_path('set_cookies').extend_query(a=1)),
                str(server_url.with_path('set_cookies').extend_query(b=2)),
                str(server_url.with_path('set_cookies').extend_query(c=3)),
            ]
        )

        visited = {call[0][0] for call in visit.call_args_list}
        assert len(visited) == 3

        session_ids = {call[0][0] for call in track_session_usage.call_args_list}
        assert len(session_ids) == 1

        session = await session_pool.get_session_by_id(session_ids.pop())
        assert session is not None
        assert {cookie['name']: cookie['value'] for cookie in session.cookies.get_cookies_as_dicts()} == {
            'a': '1',
            'b': '2',
            'c': '3',
        }


async def test_do_not_retry_on_client_errors(crawler: HttpCrawler, server_url: URL) -> None:
    await crawler.add_requests([str(server_url / 'status/400')])
    stats = await crawler.run()

    # by default, client errors are not retried
    assert stats.requests_failed == 1
    assert stats.retry_histogram == [1]
    assert stats.requests_total == 1


async def test_http_status_statistics(crawler: HttpCrawler, server_url: URL) -> None:
    await crawler.add_requests([str(server_url.with_path('status/500').with_query(id=i)) for i in range(10)])
    await crawler.add_requests([str(server_url.with_path('status/402').with_query(id=i)) for i in range(10)])
    await crawler.add_requests([str(server_url.with_path('status/403').with_query(id=i)) for i in range(10)])
    await crawler.add_requests([str(server_url.with_query(id=i)) for i in range(10)])

    await crawler.run()
    assert crawler.statistics.state.requests_with_status_code == {
        '200': 10,
        '403': 100,  # block errors change session and retry
        '402': 10,  # client errors are not retried by default
        '500': 30,  # server errors are retried by default
    }


async def test_sending_payload_as_raw_data(http_client: HttpClient, server_url: URL) -> None:
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(await context.http_response.read())
        # The post endpoint returns the provided payload in the response.
        responses.append(response)

    encoded_payload = urlencode(PAYLOAD).encode()
    request = Request.from_url(
        url=str(server_url / 'post'),
        method='POST',
        payload=encoded_payload,
    )

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'
    assert responses[0]['data'].encode() == encoded_payload, 'Response payload data does not match the sent payload.'

    # The reconstructed payload data should match the original payload. We have to flatten the values, because
    # parse_qs returns a list of values for each key.
    response_data = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(responses[0]['data']).items()}
    assert response_data == PAYLOAD, 'The reconstructed payload data does not match the sent payload.'

    assert responses[0]['json'] is None, 'Response JSON data should be empty when only raw data is sent.'
    assert responses[0]['form'] == {}, 'Response form data should be empty when only raw data is sent.'


async def test_sending_payload_as_form_data(http_client: HttpClient, server_url: URL) -> None:
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(await context.http_response.read())
        # The /post endpoint returns the provided payload in the response.
        responses.append(response)

    request = Request.from_url(
        url=str(server_url / 'post'),
        method='POST',
        headers={'content-type': 'application/x-www-form-urlencoded'},
        payload=urlencode(PAYLOAD).encode(),
    )

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'
    assert responses[0]['form'] == PAYLOAD, 'Form data in response does not match the sent payload.'

    assert responses[0]['json'] is None, 'Response JSON data should be empty when only form data is sent.'
    assert responses[0]['data'] == '', 'Response raw data should be empty when only form data is sent.'


async def test_sending_payload_as_json(http_client: HttpClient, server_url: URL) -> None:
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(await context.http_response.read())
        # The /post endpoint returns the provided payload in the response.
        responses.append(response)

    json_payload = json.dumps(PAYLOAD).encode()
    request = Request.from_url(
        url=str(server_url / 'post'),
        method='POST',
        payload=json_payload,
        headers={'content-type': 'application/json'},
    )

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'
    assert responses[0]['data'].encode() == json_payload, 'Response raw JSON data does not match the sent payload.'
    assert responses[0]['json'] == PAYLOAD, 'Response JSON data does not match the sent payload.'

    assert responses[0]['form'] == {}, 'Response form data should be empty when only JSON data is sent.'


async def test_sending_url_query_params(http_client: HttpClient, server_url: URL) -> None:
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(await context.http_response.read())
        # The /get endpoint returns the provided query parameters in the response.
        responses.append(response)

    base_url = server_url / 'get'
    query_params = {'param1': 'value1', 'param2': 'value2'}
    request = Request.from_url(url=str(base_url.extend_query(query_params)))

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'

    response_args = responses[0]['args']
    assert response_args == query_params, 'Reconstructed query params must match the original query params.'


async def test_http_crawler_pre_navigation_hooks_executed_before_request(server_url: URL) -> None:
    """Test that pre-navigation hooks are executed in correct order."""
    execution_order = []

    crawler = HttpCrawler()

    #  Register final context handler.
    @crawler.router.default_handler
    async def default_request_handler(context: HttpCrawlingContext) -> None:  # noqa: ARG001 # Unused arg in test
        execution_order.append('final handler')

    #  Register pre navigation hook.
    @crawler.pre_navigation_hook
    async def hook1(context: BasicCrawlingContext) -> None:  # noqa: ARG001 # Unused arg in test
        execution_order.append('pre-navigation-hook 1')

    #  Register pre navigation hook.
    @crawler.pre_navigation_hook
    async def hook2(context: BasicCrawlingContext) -> None:  # noqa: ARG001 # Unused arg in test
        execution_order.append('pre-navigation-hook 2')

    await crawler.run([str(server_url)])

    assert execution_order == ['pre-navigation-hook 1', 'pre-navigation-hook 2', 'final handler']


async def test_isolation_cookies(http_client: HttpClient, server_url: URL) -> None:
    """Test isolation cookies for Session with curl"""
    sessions_ids: list[str] = []
    sessions_cookies: dict[str, dict[str, str]] = {}
    response_cookies: dict[str, dict[str, str]] = {}

    crawler = HttpCrawler(
        session_pool=SessionPool(
            max_pool_size=1,
            create_session_settings={
                'max_error_score': 50,
            },
        ),
        http_client=http_client,
        max_request_retries=10,
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
    )

    @crawler.router.default_handler
    async def handler(context: HttpCrawlingContext) -> None:
        if not context.session:
            return

        sessions_ids.append(context.session.id)

        if context.request.unique_key not in {'1', '2'}:
            return

        sessions_cookies[context.session.id] = {
            cookie['name']: cookie['value'] for cookie in context.session.cookies.get_cookies_as_dicts()
        }
        response_data = json.loads(await context.http_response.read())
        response_cookies[context.session.id] = response_data.get('cookies')

        if context.request.user_data.get('retire_session'):
            context.session.retire()

    await crawler.run(
        [
            # The first request sets the cookie in the session
            str(server_url.with_path('set_cookies').extend_query(a=1)),
            # With the second request, we check the cookies in the session and set retire
            Request.from_url(str(server_url.with_path('/cookies')), unique_key='1', user_data={'retire_session': True}),
            # The third request is made with a new session to make sure it does not use another session's cookies
            Request.from_url(str(server_url.with_path('/cookies')), unique_key='2'),
        ]
    )

    assert len(sessions_cookies) == 2
    assert len(response_cookies) == 2

    assert sessions_ids[0] == sessions_ids[1]

    cookie_session_id = sessions_ids[0]
    clean_session_id = sessions_ids[2]

    assert cookie_session_id != clean_session_id

    # The initiated cookies must match in both the response and the session store
    assert sessions_cookies[cookie_session_id] == response_cookies[cookie_session_id] == {'a': '1'}

    # For a clean session, the cookie should not be in the session store or in the response
    # This way we can be sure that no cookies are being leaked through the http client
    assert sessions_cookies[clean_session_id] == response_cookies[clean_session_id] == {}


async def test_store_complex_cookies(server_url: URL) -> None:
    visit = Mock()
    track_session_usage = Mock()
    async with SessionPool(max_pool_size=1) as session_pool:
        crawler = HttpCrawler(session_pool=session_pool)

        @crawler.router.default_handler
        async def handler(context: HttpCrawlingContext) -> None:
            visit(context.request.url)
            track_session_usage(context.session.id if context.session else None)

        await crawler.run([str(server_url / 'set_complex_cookies')])

        visited = {call[0][0] for call in visit.call_args_list}
        assert len(visited) == 1

        session_ids = {call[0][0] for call in track_session_usage.call_args_list}
        assert len(session_ids) == 1

        session = await session_pool.get_session_by_id(session_ids.pop())
        assert session is not None

        session_cookies_dict = {cookie['name']: cookie for cookie in session.cookies.get_cookies_as_dicts()}

        assert len(session_cookies_dict) == 6

        # cookie string: 'basic=1; Path=/; HttpOnly; SameSite=Lax'
        assert session_cookies_dict['basic'] == {
            'name': 'basic',
            'value': '1',
            'domain': server_url.host,
            'path': '/',
            'secure': False,
            'http_only': True,
            'same_site': 'Lax',
        }

        # cookie string: 'withpath=2; Path=/html; SameSite=None'
        assert session_cookies_dict['withpath'] == {
            'name': 'withpath',
            'value': '2',
            'domain': server_url.host,
            'path': '/html',
            'secure': False,
            'http_only': False,
            'same_site': 'None',
        }

        # cookie string: 'strict=3; Path=/; SameSite=Strict'
        assert session_cookies_dict['strict'] == {
            'name': 'strict',
            'value': '3',
            'domain': server_url.host,
            'path': '/',
            'secure': False,
            'http_only': False,
            'same_site': 'Strict',
        }

        # cookie string: 'secure=4; Path=/; HttpOnly; Secure; SameSite=Strict'
        assert session_cookies_dict['secure'] == {
            'name': 'secure',
            'value': '4',
            'domain': server_url.host,
            'path': '/',
            'secure': True,
            'http_only': True,
            'same_site': 'Strict',
        }

        # cookie string: 'short=5; Path=/;'
        assert session_cookies_dict['short'] == {
            'name': 'short',
            'value': '5',
            'domain': server_url.host,
            'path': '/',
            'secure': False,
            'http_only': False,
        }

        assert session_cookies_dict['domain'] == {
            'name': 'domain',
            'value': '6',
            'domain': f'.{server_url.host}',
            'path': '/',
            'secure': False,
            'http_only': False,
        }


def test_default_logger() -> None:
    assert HttpCrawler().log.name == 'HttpCrawler'


async def test_get_snapshot(server_url: URL) -> None:
    crawler = HttpCrawler()

    snapshot = None

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        nonlocal snapshot
        snapshot = await context.get_snapshot()

    await crawler.run([str(server_url)])

    assert snapshot is not None
    assert snapshot.html is not None
    assert snapshot.html == HELLO_WORLD.decode('utf8')


async def test_error_snapshot_through_statistics(server_url: URL) -> None:
    statistics = Statistics.with_default_state(save_error_snapshots=True)
    crawler = HttpCrawler(statistics=statistics)

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        raise RuntimeError(rf'Exception /\ with file name unfriendly symbols in {context.request.url}')

    await crawler.run([str(server_url)])

    kvs = await crawler.get_key_value_store()
    kvs_content = {}
    async for key_info in kvs.iterate_keys():
        kvs_content[key_info.key] = await kvs.get_value(key_info.key)

    # One error, three time retried.
    assert crawler.statistics.error_tracker.total == 3
    assert crawler.statistics.error_tracker.unique_error_count == 1
    assert len(kvs_content) == 1
    assert key_info.key.endswith('.html')
    assert kvs_content[key_info.key] == HELLO_WORLD.decode('utf8')
