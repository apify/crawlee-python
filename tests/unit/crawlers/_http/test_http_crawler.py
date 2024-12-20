from __future__ import annotations

import json
from typing import TYPE_CHECKING, Callable
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlencode

import pytest
import respx
from httpx import Response

from crawlee._request import Request
from crawlee.crawlers import HttpCrawler
from crawlee.http_clients import CurlImpersonateHttpClient, HttpxHttpClient
from crawlee.sessions import SessionPool

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from yarl import URL

    from crawlee._types import BasicCrawlingContext
    from crawlee.crawlers import HttpCrawlingContext
    from crawlee.http_clients._base import BaseHttpClient

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
async def crawler(mock_request_handler: Callable[[HttpCrawlingContext], Awaitable[None]]) -> HttpCrawler:
    return HttpCrawler(request_handler=mock_request_handler)


@pytest.fixture
async def crawler_without_retries(
    mock_request_handler: Callable[[HttpCrawlingContext], Awaitable[None]],
) -> HttpCrawler:
    return HttpCrawler(
        request_handler=mock_request_handler,
        retry_on_blocked=False,
        max_request_retries=0,
    )


@pytest.fixture
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

        mock.get('/bad_request', name='bad_request_endpoint').return_value = Response(
            400,
            text="""<html>
                <head>
                    <title>Bad request</title>
                </head>
            </html>""",
        )

        mock.get('/403', name='403_endpoint').return_value = Response(
            403,
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


async def test_fetches_html(
    crawler: HttpCrawler,
    mock_request_handler: AsyncMock,
    server: respx.MockRouter,
) -> None:
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


@pytest.mark.parametrize(
    ('additional_http_error_status_codes', 'ignore_http_error_status_codes', 'expected_number_error'),
    [
        ([], [], 1),
        ([403], [], 3),
        ([], [403], 0),
        ([403], [403], 3),
    ],
    ids=[
        'default_behavior',  # error without retry for all 4xx statuses
        'additional_status_codes',  # make retry for codes in `additional_http_error_status_codes` list
        'ignore_error_status_codes',  # take as successful status codes from the `ignore_http_error_status_codes` list
        'additional_and_ignore',  # check precedence for `additional_http_error_status_codes`
    ],
)
async def test_handles_client_errors(
    additional_http_error_status_codes: list[int],
    ignore_http_error_status_codes: list[int],
    expected_number_error: int,
    mock_request_handler: AsyncMock,
    server: respx.MockRouter,
) -> None:
    crawler = HttpCrawler(
        request_handler=mock_request_handler,
        additional_http_error_status_codes=additional_http_error_status_codes,
        ignore_http_error_status_codes=ignore_http_error_status_codes,
        max_request_retries=3,
    )

    await crawler.add_requests(['https://test.io/403'])
    await crawler.run()

    assert crawler.statistics.error_tracker.total == expected_number_error

    # Request handler should not be called for error status codes.
    if expected_number_error:
        mock_request_handler.assert_not_called()
    else:
        mock_request_handler.assert_called()

    assert server['403_endpoint'].called


async def test_handles_server_error(
    crawler: HttpCrawler, mock_request_handler: AsyncMock, server: respx.MockRouter
) -> None:
    await crawler.add_requests(['https://test.io/500'])
    await crawler.run()

    mock_request_handler.assert_not_called()
    assert server['500_endpoint'].called


async def test_stores_cookies(httpbin: URL) -> None:
    visit = Mock()
    track_session_usage = Mock()

    async with SessionPool(max_pool_size=1) as session_pool:
        crawler = HttpCrawler(
            # /cookies/set might redirect us to a page that we can't access - no problem, we only care about cookies
            ignore_http_error_status_codes=[401],
            session_pool=session_pool,
        )

        @crawler.router.default_handler
        async def handler(context: HttpCrawlingContext) -> None:
            visit(context.request.url)
            track_session_usage(context.session.id if context.session else None)

        await crawler.run(
            [
                str(httpbin.with_path('/cookies/set').extend_query(a=1)),
                str(httpbin.with_path('/cookies/set').extend_query(b=2)),
                str(httpbin.with_path('/cookies/set').extend_query(c=3)),
            ]
        )

        visited = {call[0][0] for call in visit.call_args_list}
        assert len(visited) == 3

        session_ids = {call[0][0] for call in track_session_usage.call_args_list}
        assert len(session_ids) == 1

        session = await session_pool.get_session_by_id(session_ids.pop())
        assert session is not None
        assert session.cookies == {'a': '1', 'b': '2', 'c': '3'}


async def test_do_not_retry_on_client_errors(crawler: HttpCrawler, server: respx.MockRouter) -> None:
    await crawler.add_requests(['https://test.io/bad_request'])
    stats = await crawler.run()

    # by default, client errors are not retried
    assert stats.requests_failed == 1
    assert stats.retry_histogram == [1]
    assert stats.requests_total == 1

    assert len(server['bad_request_endpoint'].calls) == 1


async def test_http_status_statistics(crawler: HttpCrawler, server: respx.MockRouter) -> None:
    await crawler.add_requests([f'https://test.io/500?id={i}' for i in range(10)])
    await crawler.add_requests([f'https://test.io/403?id={i}' for i in range(10)])
    await crawler.add_requests([f'https://test.io/html?id={i}' for i in range(10)])

    await crawler.run()

    assert crawler.statistics.state.requests_with_status_code == {
        '200': 10,
        '403': 10,  # client errors are not retried by default
        '500': 30,  # server errors are retried by default
    }

    assert len(server['html_endpoint'].calls) == 10
    assert len(server['403_endpoint'].calls) == 10
    assert len(server['500_endpoint'].calls) == 30


@pytest.mark.parametrize(
    'http_client_class',
    [CurlImpersonateHttpClient, HttpxHttpClient],
    ids=['curl', 'httpx'],
)
async def test_sending_payload_as_raw_data(http_client_class: type[BaseHttpClient], httpbin: URL) -> None:
    http_client = http_client_class()
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(context.http_response.read())
        # The httpbin.org/post endpoint returns the provided payload in the response.
        responses.append(response)

    encoded_payload = urlencode(PAYLOAD).encode()
    request = Request.from_url(
        url=str(httpbin / 'post'),
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


@pytest.mark.parametrize(
    'http_client_class',
    [CurlImpersonateHttpClient, HttpxHttpClient],
    ids=['curl', 'httpx'],
)
async def test_sending_payload_as_form_data(http_client_class: type[BaseHttpClient], httpbin: URL) -> None:
    http_client = http_client_class()
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(context.http_response.read())
        # The httpbin.org/post endpoint returns the provided payload in the response.
        responses.append(response)

    request = Request.from_url(
        url=str(httpbin / 'post'),
        method='POST',
        headers={'content-type': 'application/x-www-form-urlencoded'},
        payload=urlencode(PAYLOAD).encode(),
    )

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'
    assert responses[0]['form'] == PAYLOAD, 'Form data in response does not match the sent payload.'

    assert responses[0]['json'] is None, 'Response JSON data should be empty when only form data is sent.'
    assert responses[0]['data'] == '', 'Response raw data should be empty when only form data is sent.'


@pytest.mark.parametrize(
    'http_client_class',
    [CurlImpersonateHttpClient, HttpxHttpClient],
    ids=['curl', 'httpx'],
)
async def test_sending_payload_as_json(http_client_class: type[BaseHttpClient], httpbin: URL) -> None:
    http_client = http_client_class()
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(context.http_response.read())
        # The httpbin.org/post endpoint returns the provided payload in the response.
        responses.append(response)

    json_payload = json.dumps(PAYLOAD).encode()
    request = Request.from_url(
        url=str(httpbin / 'post'),
        method='POST',
        payload=json_payload,
        headers={'content-type': 'application/json'},
    )

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'
    assert responses[0]['data'].encode() == json_payload, 'Response raw JSON data does not match the sent payload.'
    assert responses[0]['json'] == PAYLOAD, 'Response JSON data does not match the sent payload.'

    assert responses[0]['form'] == {}, 'Response form data should be empty when only JSON data is sent.'


@pytest.mark.parametrize(
    'http_client_class',
    [CurlImpersonateHttpClient, HttpxHttpClient],
    ids=['curl', 'httpx'],
)
async def test_sending_url_query_params(http_client_class: type[BaseHttpClient], httpbin: URL) -> None:
    http_client = http_client_class()
    crawler = HttpCrawler(http_client=http_client)
    responses = []

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        response = json.loads(context.http_response.read())
        # The httpbin.org/get endpoint returns the provided query parameters in the response.
        responses.append(response)

    base_url = httpbin / 'get'
    query_params = {'param1': 'value1', 'param2': 'value2'}
    request = Request.from_url(url=str(base_url.extend_query(query_params)))

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'

    response_args = responses[0]['args']
    assert response_args == query_params, 'Reconstructed query params must match the original query params.'


@respx.mock
async def test_http_crawler_pre_navigation_hooks_executed_before_request() -> None:
    """Test that pre-navigation hooks are executed in correct order."""
    execution_order = []
    test_url = 'http://www.something.com'

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

    def mark_request_execution(request: Request) -> Response:  # noqa: ARG001 # Unused arg in test
        # Helper function to track execution order.
        execution_order.append('request')
        return Response(200)

    respx.get(test_url).mock(side_effect=mark_request_execution)
    await crawler.run([Request.from_url(url=test_url)])

    assert execution_order == ['pre-navigation-hook 1', 'pre-navigation-hook 2', 'request', 'final handler']
