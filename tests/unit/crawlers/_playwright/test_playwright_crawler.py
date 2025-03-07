# TODO: The current PlaywrightCrawler tests rely on external websites. It means they can fail or take more time
# due to network issues. To enhance test stability and reliability, we should mock the network requests.
# https://github.com/apify/crawlee-python/issues/197

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal
from unittest import mock
from unittest.mock import Mock

import pytest

from crawlee import ConcurrencySettings, Glob, HttpHeaders, Request, RequestTransformAction
from crawlee.crawlers import PlaywrightCrawler
from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    FingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import BROWSER_TYPE_HEADER_KEYWORD
from crawlee.proxy_configuration import ProxyConfiguration
from crawlee.sessions import SessionPool

if TYPE_CHECKING:
    from pathlib import Path

    from yarl import URL

    from crawlee._request import RequestOptions
    from crawlee.browsers._types import BrowserType
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
    # www.crawlee.dev create a redirect to crawlee.dev
    requests = ['https://www.crawlee.dev/docs/examples']
    crawler = PlaywrightCrawler(max_requests_per_crawl=11)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links(include=[Glob('https://crawlee.dev/docs/examples/**')])

    await crawler.run(requests)

    first_visited = visit.call_args_list[0][0][0]
    visited: set[str] = {call[0][0] for call in visit.call_args_list[1:]}

    # The first link visited use original domain
    assert first_visited == 'https://www.crawlee.dev/docs/examples'
    assert len(visited) >= 10
    # All other links must have a domain name after the redirect
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
    crawler = PlaywrightCrawler(max_request_retries=4, request_handler=mock.AsyncMock())

    error_handler = mock.AsyncMock(return_value=None)
    crawler.error_handler(error_handler)

    failed_handler = mock.AsyncMock(return_value=None)
    crawler.failed_request_handler(failed_handler)

    await crawler.run(['https://this-does-not-exist-22343434.com'])
    assert error_handler.call_count == 3
    assert failed_handler.call_count == 1


async def test_redirect_handling() -> None:
    # Set up a dummy crawler that tracks visited URLs
    crawler = PlaywrightCrawler()
    handled_urls = set[str]()

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        # Emulate server response to prevent Playwright from making real requests
        await context.page.route(
            '**/redirect', lambda route: route.fulfill(status=301, headers={'Location': 'https://apify.com'})
        )
        await context.page.route('https://apify.com/**', lambda route: route.fulfill(status=200))

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        handled_urls.add(context.request.loaded_url or '')

    # Request with redirects to apify.com
    request = Request.from_url(url='https://test.com/redirect')

    # Ensure that the request uses the same origin strategy - apify.com will be considered out of scope
    request.crawlee_data.enqueue_strategy = 'same-origin'

    # No URLs should be visited in the run
    await crawler.run([request])
    assert handled_urls == set()


@pytest.mark.parametrize(
    'fingerprint_generator',
    [
        pytest.param(None, id='No fingerprint generator. Headers generated by header generator.'),
        pytest.param(
            DefaultFingerprintGenerator(header_options=HeaderGeneratorOptions(browsers=['chromium'])),
            id='Explicitly passed fingerprint generator.',
        ),
        pytest.param('default', id='Default fingerprint generator.'),
    ],
)
async def test_chromium_headless_headers(
    header_network: dict, fingerprint_generator: None | FingerprintGenerator | Literal['default']
) -> None:
    browser_type: BrowserType = 'chromium'
    crawler = PlaywrightCrawler(headless=True, browser_type=browser_type, fingerprint_generator=fingerprint_generator)
    headers = dict[str, str]()

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        # Emulate server response to prevent Playwright from making real requests
        await context.page.route(
            '**/*', lambda route: route.fulfill(status=200, body=json.dumps(route.request.headers))
        )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        response_headers = json.loads(response)

        for key, val in response_headers.items():
            headers[key] = val

    await crawler.run(['https://test.com'])

    user_agent = headers.get('user-agent')
    assert user_agent in get_available_header_values(header_network, {'user-agent', 'User-Agent'})
    assert any(keyword in user_agent for keyword in BROWSER_TYPE_HEADER_KEYWORD[browser_type]), user_agent

    assert headers.get('sec-ch-ua') in get_available_header_values(header_network, 'sec-ch-ua')
    assert headers.get('sec-ch-ua-mobile') in get_available_header_values(header_network, 'sec-ch-ua-mobile')
    assert headers.get('sec-ch-ua-platform') in get_available_header_values(header_network, 'sec-ch-ua-platform')

    assert 'headless' not in headers['sec-ch-ua'].lower()
    assert 'headless' not in headers['user-agent'].lower()


async def test_firefox_headless_headers(header_network: dict) -> None:
    browser_type: BrowserType = 'firefox'
    crawler = PlaywrightCrawler(headless=True, browser_type=browser_type)
    headers = dict[str, str]()

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        # Emulate server response to prevent Playwright from making real requests
        await context.page.route(
            '**/*', lambda route: route.fulfill(status=200, body=json.dumps(route.request.headers))
        )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        response_headers = json.loads(response)

        for key, val in response_headers.items():
            headers[key] = val

    await crawler.run(['https://test.com'])

    assert 'user-agent' in headers
    assert 'sec-ch-ua' not in headers
    assert 'sec-ch-ua-mobile' not in headers
    assert 'sec-ch-ua-platform' not in headers

    assert 'headless' not in headers['user-agent'].lower()

    user_agent = headers.get('user-agent')
    assert user_agent in get_available_header_values(header_network, {'user-agent', 'User-Agent'})
    assert any(keyword in user_agent for keyword in BROWSER_TYPE_HEADER_KEYWORD[browser_type])


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


async def test_pre_navigation_hook() -> None:
    crawler = PlaywrightCrawler(request_handler=mock.AsyncMock())
    visit = mock.Mock()

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        visit()
        await context.page.route('**/*', lambda route: route.fulfill(status=200))

    await crawler.run(['https://test.com', 'https://test.io'])

    assert visit.call_count == 2


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


@pytest.mark.parametrize(
    'use_incognito_pages',
    [
        pytest.param(False, id='without use_incognito_pages'),
        pytest.param(True, id='with use_incognito_pages'),
    ],
)
async def test_isolation_cookies(*, use_incognito_pages: bool) -> None:
    sessions_ids: list[str] = []
    sessions_cookies: dict[str, dict[str, str]] = {}
    response_cookies: dict[str, dict[str, str]] = {}

    crawler = PlaywrightCrawler(
        session_pool=SessionPool(max_pool_size=1),
        use_incognito_pages=use_incognito_pages,
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
    )

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        # Emulate server response to prevent Playwright from making real requests
        await context.page.route(
            '**/cookies_set', lambda route: route.fulfill(status=200, headers={'Set-Cookie': 'a=1; Path=/'})
        )

        await context.page.route(
            '**/cookies_get',
            lambda route: route.fulfill(
                status=200, body=json.dumps({'cookies': route.request.headers.get('cookie', '')})
            ),
        )

    @crawler.router.default_handler
    async def handler(context: PlaywrightCrawlingContext) -> None:
        if not context.session:
            return

        sessions_ids.append(context.session.id)

        if context.request.unique_key not in {'1', '2'}:
            return

        sessions_cookies[context.session.id] = {
            cookie['name']: cookie['value'] for cookie in context.session.cookies.get_cookies_as_dicts()
        }
        response_data = json.loads(await context.response.text())
        response_cookies[context.session.id] = response_data.get('cookies')

        if context.request.user_data.get('retire_session'):
            context.session.retire()

    await crawler.run(
        [
            # The first request sets the cookie in the session
            'https://test.com/cookies_set',
            # With the second request, we check the cookies in the session and set retire
            Request.from_url('https://test.com/cookies_get', unique_key='1', user_data={'retire_session': True}),
            Request.from_url('https://test.com/cookies_get', unique_key='2'),
        ]
    )

    assert len(sessions_cookies) == 2
    assert len(response_cookies) == 2

    assert sessions_ids[0] == sessions_ids[1]

    cookie_session_id = sessions_ids[0]
    clean_session_id = sessions_ids[2]

    assert cookie_session_id != clean_session_id

    # When using `use_incognito_pages` there should be full cookie isolation
    if use_incognito_pages:
        # The initiated cookies must match in both the response and the session store
        assert sessions_cookies[cookie_session_id] == {'a': '1'}
        assert response_cookies[cookie_session_id] == 'a=1'

        # For a clean session, the cookie should not be in the sesstion store or in the response
        # This way we can be sure that no cookies are being leaked through the http client
        assert sessions_cookies[clean_session_id] == {}
        assert response_cookies[clean_session_id] == ''
    # Without `use_incognito_pages` we will have access to the session cookie,
    # but there will be a cookie leak via PlaywrightContext
    else:
        # The initiated cookies must match in both the response and the session store
        assert sessions_cookies[cookie_session_id] == {'a': '1'}
        assert response_cookies[cookie_session_id] == 'a=1'

        # PlaywrightContext makes cookies shared by all sessions that work with it.
        # So in this case a clean session contains the same cookies
        assert sessions_cookies[clean_session_id] == {'a': '1'}
        assert response_cookies[clean_session_id] == 'a=1'


async def test_custom_fingerprint_uses_generator_options(httpbin: URL) -> None:
    min_width = 300
    max_width = 600
    min_height = 500
    max_height = 1200

    fingerprint_generator = DefaultFingerprintGenerator(
        header_options=HeaderGeneratorOptions(browsers=['firefox'], operating_systems=['android']),
        screen_options=ScreenOptions(
            min_width=min_width, max_width=max_width, min_height=min_height, max_height=max_height
        ),
    )

    crawler = PlaywrightCrawler(headless=True, fingerprint_generator=fingerprint_generator)

    fingerprints = dict[str, Any]()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        for relevant_key in (
            'window.navigator.userAgent',
            'window.navigator.userAgentData',
            'window.screen.height',
            'window.screen.width',
        ):
            fingerprints[relevant_key] = await context.page.evaluate(f'()=>{relevant_key}')

    await crawler.run([Request.from_url(str(httpbin / 'get'))])

    assert 'Firefox' in fingerprints['window.navigator.userAgent']
    assert fingerprints['window.navigator.userAgentData']['platform'] == 'Android'
    assert min_width <= int(fingerprints['window.screen.width']) <= max_width
    assert min_height <= int(fingerprints['window.screen.height']) <= max_height


async def test_custom_fingerprint_matches_header_user_agent() -> None:
    """Test that generated fingerprint and header have matching user agent."""

    crawler = PlaywrightCrawler(headless=True, fingerprint_generator=DefaultFingerprintGenerator())
    response_headers = dict[str, str]()
    fingerprints = dict[str, str]()

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        # Emulate server response to prevent Playwright from making real requests
        await context.page.route(
            '**/*', lambda route: route.fulfill(status=200, body=json.dumps(route.request.headers))
        )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        context_response_headers = dict(json.loads(response))

        response_headers['User-Agent'] = context_response_headers['user-agent']
        fingerprints['window.navigator.userAgent'] = await context.page.evaluate('()=>window.navigator.userAgent')

    await crawler.run(['https://test.com'])

    assert response_headers['User-Agent'] == fingerprints['window.navigator.userAgent']


async def test_ignore_http_error_status_codes() -> None:
    """Test that error codes that would normally trigger session error can be ignored."""
    crawler = PlaywrightCrawler(ignore_http_error_status_codes={403})

    mocked_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        mocked_handler(context.request.url)

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        # Emulate server response to prevent Playwright from making real requests
        await context.page.route('**/*', lambda route: route.fulfill(status=403))

    await crawler.run(['https://test.com'])

    mocked_handler.assert_called_once_with('https://test.com')


async def test_additional_http_error_status_codes() -> None:
    """Test that use of `additional_http_error_status_codes` can raise error on common status code."""
    crawler = PlaywrightCrawler(additional_http_error_status_codes={200})

    mocked_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        mocked_handler(context.request.url)

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        # Emulate server response to prevent Playwright from making real requests
        await context.page.route('**/*', lambda route: route.fulfill(status=200))

    await crawler.run(['https://test.com'])

    mocked_handler.assert_not_called()


async def test_launch_with_user_data_dir(tmp_path: Path) -> None:
    """Check that the persist context is created in the specified folder in `user_data_dir`."""
    check_path = tmp_path / 'Default'
    crawler = PlaywrightCrawler(
        headless=True, user_data_dir=tmp_path, request_handler=mock.AsyncMock(return_value=None)
    )

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        await context.page.route('**/*', lambda route: route.fulfill(status=200))

    assert not check_path.exists()

    await crawler.run(['https://test.io'])

    assert check_path.exists()


async def test_launch_with_user_data_dir_and_fingerprint(tmp_path: Path) -> None:
    """Check that the persist context works with fingerprints."""
    check_path = tmp_path / 'Default'
    fingerprints = dict[str, str]()

    crawler = PlaywrightCrawler(
        headless=True,
        user_data_dir=tmp_path,
        request_handler=mock.AsyncMock(return_value=None),
        fingerprint_generator=DefaultFingerprintGenerator(),
    )

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        fingerprints['window.navigator.userAgent'] = await context.page.evaluate('()=>window.navigator.userAgent')
        await context.page.route('**/*', lambda route: route.fulfill(status=200))

    assert not check_path.exists()

    await crawler.run(['https://test.io'])

    assert check_path.exists()

    assert fingerprints['window.navigator.userAgent']
    assert 'headless' not in fingerprints['window.navigator.userAgent'].lower()
