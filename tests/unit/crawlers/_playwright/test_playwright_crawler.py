# TODO: The current PlaywrightCrawler tests rely on external websites. It means they can fail or take more time
# due to network issues. To enhance test stability and reliability, we should mock the network requests.
# https://github.com/apify/crawlee-python/issues/197

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest import mock

from crawlee import Glob, HttpHeaders, Request, RequestTransformAction
from crawlee._types import EnqueueStrategy
from crawlee.crawlers import PlaywrightCrawler
from crawlee.fingerprint_suite._consts import (
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM,
    PW_CHROMIUM_HEADLESS_DEFAULT_USER_AGENT,
    PW_FIREFOX_HEADLESS_DEFAULT_USER_AGENT,
)
from crawlee.proxy_configuration import ProxyConfiguration

if TYPE_CHECKING:
    from yarl import URL

    from crawlee._request import RequestOptions
    from crawlee.crawlers import PlaywrightCrawlingContext, PlaywrightPreNavCrawlingContext


async def test_basic_request(httpbin: URL) -> None:
    requests = [str(httpbin.with_path('/'))]
    crawler = PlaywrightCrawler()
    result: dict = {}

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        assert context.page is not None
        result['request_url'] = context.request.url
        result['page_url'] = context.page.url
        result['page_title'] = await context.page.title()
        result['page_content'] = await context.page.content()

    await crawler.run(requests)

    assert result.get('request_url') == result.get('page_url') == requests[0]
    assert 'httpbin' in result.get('page_title', '')
    assert '<html' in result.get('page_content', '')  # there is some HTML content


async def test_enqueue_links() -> None:
    requests = ['https://crawlee.dev/docs/examples']
    crawler = PlaywrightCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links(include=[Glob('https://crawlee.dev/docs/examples/**')])

    await crawler.run(requests)

    visited: set[str] = {call[0][0] for call in visit.call_args_list}

    assert len(visited) >= 10
    assert all(url.startswith('https://crawlee.dev/docs/examples') for url in visited)


async def test_enqueue_links_with_transform_request_function() -> None:
    crawler = PlaywrightCrawler()
    visit = mock.Mock()
    headers = []

    def test_transform_request_function(request: RequestOptions) -> RequestOptions | RequestTransformAction:
        if request['url'] == 'https://crawlee.dev/python/docs/introduction':
            request['headers'] = HttpHeaders({'transform-header': 'my-header'})
            return request
        return 'skip'

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        visit(context.request.url)
        headers.append(context.request.headers)
        await context.enqueue_links(transform_request_function=test_transform_request_function)

    await crawler.run(['https://crawlee.dev/python'])

    visited = {call[0][0] for call in visit.call_args_list}

    assert visited == {'https://crawlee.dev/python', 'https://crawlee.dev/python/docs/introduction'}

    # all urls added to `enqueue_links` must have a custom header
    assert headers[1]['transform-header'] == 'my-header'


async def test_nonexistent_url_invokes_error_handler() -> None:
    crawler = PlaywrightCrawler(max_request_retries=4)

    error_handler = mock.AsyncMock(return_value=None)
    crawler.error_handler(error_handler)

    failed_handler = mock.AsyncMock(return_value=None)
    crawler.failed_request_handler(failed_handler)

    @crawler.router.default_handler
    async def request_handler(_context: PlaywrightCrawlingContext) -> None:
        pass

    await crawler.run(['https://this-does-not-exist-22343434.com'])
    assert error_handler.call_count == 3
    assert failed_handler.call_count == 1


async def test_redirect_handling(httpbin: URL) -> None:
    # Set up a dummy crawler that tracks visited URLs
    crawler = PlaywrightCrawler()
    handled_urls = set[str]()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        handled_urls.add(context.request.loaded_url or '')

    # Craft a request that points to httpbin initially, but redirects to apify.com
    request = Request.from_url(
        url=str((httpbin / 'redirect-to').update_query(url='https://apify.com')),
    )

    # Ensure that the request uses the SAME_ORIGIN strategy - apify.com will be considered out of scope
    request.crawlee_data.enqueue_strategy = EnqueueStrategy.SAME_ORIGIN

    # No URLs should be visited in the run
    await crawler.run([request])
    assert handled_urls == set()


async def test_chromium_headless_headers(httpbin: URL) -> None:
    crawler = PlaywrightCrawler(headless=True, browser_type='chromium')
    headers = dict[str, str]()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        response_headers = dict(json.loads(response)).get('headers', {})

        for key, val in response_headers.items():
            headers[key] = val

    await crawler.run([str(httpbin / 'get')])

    assert 'User-Agent' in headers
    assert 'Sec-Ch-Ua' in headers
    assert 'Sec-Ch-Ua-Mobile' in headers
    assert 'Sec-Ch-Ua-Platform' in headers

    assert 'headless' not in headers['Sec-Ch-Ua'].lower()
    assert 'headless' not in headers['User-Agent'].lower()

    assert headers['Sec-Ch-Ua'] == PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA
    assert headers['Sec-Ch-Ua-Mobile'] == PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE
    assert headers['Sec-Ch-Ua-Platform'] == PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM
    assert headers['User-Agent'] == PW_CHROMIUM_HEADLESS_DEFAULT_USER_AGENT


async def test_firefox_headless_headers(httpbin: URL) -> None:
    crawler = PlaywrightCrawler(headless=True, browser_type='firefox')
    headers = dict[str, str]()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        response_headers = dict(json.loads(response)).get('headers', {})

        for key, val in response_headers.items():
            headers[key] = val

    await crawler.run([str(httpbin / 'get')])

    assert 'User-Agent' in headers
    assert 'Sec-Ch-Ua' not in headers
    assert 'Sec-Ch-Ua-Mobile' not in headers
    assert 'Sec-Ch-Ua-Platform' not in headers

    assert 'headless' not in headers['User-Agent'].lower()

    assert headers['User-Agent'] == PW_FIREFOX_HEADLESS_DEFAULT_USER_AGENT


async def test_custom_headers(httpbin: URL) -> None:
    crawler = PlaywrightCrawler()
    response_headers = dict[str, str]()
    request_headers = {'Power-Header': 'ring', 'Library': 'storm', 'My-Test-Header': 'fuzz'}

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        context_response_headers = dict(json.loads(response)).get('headers', {})

        for key, val in context_response_headers.items():
            response_headers[key] = val

    await crawler.run([Request.from_url(str(httpbin / 'get'), headers=request_headers)])

    assert response_headers.get('Power-Header') == request_headers['Power-Header']
    assert response_headers.get('Library') == request_headers['Library']
    assert response_headers.get('My-Test-Header') == request_headers['My-Test-Header']


async def test_pre_navigation_hook(httpbin: URL) -> None:
    crawler = PlaywrightCrawler()
    mock_hook = mock.AsyncMock(return_value=None)

    crawler.pre_navigation_hook(mock_hook)

    @crawler.router.default_handler
    async def request_handler(_context: PlaywrightCrawlingContext) -> None:
        pass

    await crawler.run(['https://example.com', str(httpbin)])

    assert mock_hook.call_count == 2


async def test_proxy_set() -> None:
    # Configure crawler with proxy settings
    proxy_value = 'http://1111:1111'
    crawler = PlaywrightCrawler(proxy_configuration=ProxyConfiguration(proxy_urls=[proxy_value]))

    handler_data = {}

    mock_handler = mock.AsyncMock(return_value=None)
    crawler.router.default_handler(mock_handler)

    # Use pre_navigation_hook to verify proxy and configure playwright route
    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        if context.proxy_info:
            # Store information about the used proxy
            handler_data['proxy'] = context.proxy_info.url

        # Emulate server response to prevent Playwright from making real requests
        await context.page.route('**/*', lambda route: route.fulfill(status=200))

    await crawler.run(['https://test.com'])

    assert handler_data.get('proxy') == proxy_value
