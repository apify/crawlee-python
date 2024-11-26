from __future__ import annotations

import json
from typing import TYPE_CHECKING, Callable
from unittest.mock import AsyncMock, Mock, call
from urllib.parse import parse_qs, urlencode

import pytest
import respx
from httpx import URL, Response

from crawlee._request import Request
from crawlee._types import BasicCrawlingContext
from crawlee.http_clients._httpx import HttpxHttpClient
from crawlee.http_clients.curl_impersonate import CurlImpersonateHttpClient
from crawlee.http_crawler import HttpCrawler
from crawlee.sessions import SessionPool
from crawlee.storages import RequestList

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from crawlee.http_clients._base import BaseHttpClient
    from crawlee.http_crawler._http_crawling_context import HttpCrawlingContext


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
    return HttpCrawler(
        request_handler=mock_request_handler,
        request_provider=RequestList(),
    )


@pytest.fixture
async def crawler_without_retries(
    mock_request_handler: Callable[[HttpCrawlingContext], Awaitable[None]],
) -> HttpCrawler:
    return HttpCrawler(
        request_handler=mock_request_handler,
        request_provider=RequestList(),
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


async def test_handles_client_errors(
    crawler_without_retries: HttpCrawler,
    mock_request_handler: AsyncMock,
    server: respx.MockRouter,
) -> None:
    crawler = crawler_without_retries

    await crawler.add_requests(['https://test.io/404'])
    await crawler.run()

    # Request handler should not be called for error status codes.
    mock_request_handler.assert_not_called()
    assert server['404_endpoint'].called


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
            request_provider=RequestList(
                [
                    str(httpbin.copy_with(path='/cookies/set').copy_set_param('a', '1')),
                    str(httpbin.copy_with(path='/cookies/set').copy_set_param('b', '2')),
                    str(httpbin.copy_with(path='/cookies/set').copy_set_param('c', '3')),
                ]
            ),
            # /cookies/set might redirect us to a page that we can't access - no problem, we only care about cookies
            ignore_http_error_status_codes=[401],
            session_pool=session_pool,
        )

        @crawler.router.default_handler
        async def handler(context: HttpCrawlingContext) -> None:
            visit(context.request.url)
            track_session_usage(context.session.id if context.session else None)

        await crawler.run()

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
    await crawler.add_requests([f'https://test.io/404?id={i}' for i in range(10)])
    await crawler.add_requests([f'https://test.io/html?id={i}' for i in range(10)])

    await crawler.run()

    assert crawler.statistics.state.requests_with_status_code == {
        '200': 10,
        '404': 10,  # client errors are not retried by default
        '500': 30,  # server errors are retried by default
    }

    assert len(server['html_endpoint'].calls) == 10
    assert len(server['404_endpoint'].calls) == 10
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
        url=str(httpbin.copy_with(path='/post')),
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
        url=str(httpbin.copy_with(path='/post')),
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
        url=str(httpbin.copy_with(path='/post')),
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

    base_url = httpbin.copy_with(path='/get')
    query_params = {'param1': 'value1', 'param2': 'value2'}
    request = Request.from_url(url=str(base_url.copy_merge_params(query_params)))

    await crawler.run([request])

    assert len(responses) == 1, 'Request handler should be called exactly once.'

    response_args = responses[0]['args']
    assert response_args == query_params, 'Reconstructed query params must match the original query params.'


@respx.mock
async def test_http_crawler_pre_navigation_hooks():
    test_url_with_registered_label = "http://www.something1.com"
    test_url_without_label = "http://www.something2.com"
    test_url_with_unregistered_label = "http://www.something3.com"

    handler_for_registered_hook = Mock()
    default_handler = Mock()
    registered_label = "Bla"
    unregistered_label = "Ble"

    crawler = HttpCrawler()

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        pass

    @crawler.pre_navigation_router.handler(registered_label)
    async def request_handler(context: BasicCrawlingContext) -> None:
        handler_for_registered_hook(context.request.url, context.request.label)

    @crawler.pre_navigation_router.default_handler
    async def request_handler(context: BasicCrawlingContext) -> None:
        default_handler(context.request.url, context.request.label)

    for test_url in (test_url_with_registered_label, test_url_without_label, test_url_with_unregistered_label):
        respx.get(test_url).mock(return_value=Response(200))

    requests = [
        Request.from_url(label= registered_label, url=test_url_with_registered_label),
        Request.from_url(url=test_url_without_label),
        Request.from_url(label=unregistered_label, url=test_url_with_unregistered_label)
        ]


    await crawler.run(requests)

    handler_for_registered_hook.assert_called_once_with(test_url_with_registered_label, registered_label)
    default_handler.assert_has_calls([
        call(test_url_without_label, None),
        call(test_url_with_unregistered_label, unregistered_label),
    ])
