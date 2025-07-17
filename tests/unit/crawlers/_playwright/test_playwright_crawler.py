# TODO: The current PlaywrightCrawler tests rely on external websites. It means they can fail or take more time
# due to network issues. To enhance test stability and reliability, we should mock the network requests.
# https://github.com/apify/crawlee-python/issues/197

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal
from unittest import mock
from unittest.mock import Mock

import pytest

from crawlee import (
    ConcurrencySettings,
    Glob,
    HttpHeaders,
    Request,
    RequestTransformAction,
    SkippedReason,
    service_locator,
)
from crawlee.configuration import Configuration
from crawlee.crawlers import PlaywrightCrawler
from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    FingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import BROWSER_TYPE_HEADER_KEYWORD
from crawlee.fingerprint_suite._header_generator import fingerprint_browser_type_from_playwright_browser_type
from crawlee.http_clients import HttpxHttpClient
from crawlee.proxy_configuration import ProxyConfiguration
from crawlee.sessions import Session, SessionPool
from crawlee.statistics import Statistics
from crawlee.statistics._error_snapshotter import ErrorSnapshotter
from tests.unit.server_endpoints import GENERIC_RESPONSE, HELLO_WORLD

if TYPE_CHECKING:
    from pathlib import Path

    from yarl import URL

    from crawlee._request import RequestOptions
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.browsers._types import BrowserType
    from crawlee.crawlers import PlaywrightCrawlingContext, PlaywrightPreNavCrawlingContext


@pytest.mark.parametrize(
    ('method', 'path', 'payload'),
    [
        pytest.param('GET', 'get', None, id='get request'),
        pytest.param('POST', 'post', None, id='post request'),
        pytest.param('POST', 'post', b'Hello, world!', id='post request with payload'),
    ],
)
async def test_basic_request(method: HttpMethod, path: str, payload: HttpPayload, server_url: URL) -> None:
    requests = [Request.from_url(str(server_url / path), method=method, payload=payload)]
    crawler = PlaywrightCrawler()
    result: dict = {}

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        assert context.page is not None
        result['request_url'] = context.request.url
        result['page_url'] = context.page.url
        result['page_content'] = await context.page.content()

    await crawler.run(requests)
    assert result.get('request_url') == result.get('page_url') == requests[0].url
    assert (payload.decode() if payload else '') in result.get('page_content', '')


async def test_enqueue_links(redirect_server_url: URL, server_url: URL) -> None:
    redirect_target = str(server_url / 'start_enqueue')
    redirect_url = str(redirect_server_url.with_path('redirect').with_query(url=redirect_target))
    requests = [redirect_url]
    crawler = PlaywrightCrawler()
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links()

    await crawler.run(requests)

    first_visited = visit.call_args_list[0][0][0]
    visited = {call[0][0] for call in visit.call_args_list[1:]}

    assert first_visited == redirect_url
    assert visited == {
        str(server_url / 'sub_index'),
        str(server_url / 'page_1'),
        str(server_url / 'page_2'),
        str(server_url / 'page_3'),
    }


async def test_enqueue_links_with_incompatible_kwargs_raises_error(server_url: URL) -> None:
    """Call `enqueue_links` with arguments that can't be used together."""
    crawler = PlaywrightCrawler(max_request_retries=1)
    exceptions = []

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        await context.page.route('**/*', lambda route: route.fulfill(status=200))

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        try:
            await context.enqueue_links(requests=[Request.from_url('https://www.whatever.com')], selector='a')  # type:ignore[call-overload]  # Testing runtime enforcement of the overloads.
        except Exception as e:
            exceptions.append(e)

    await crawler.run([str(server_url)])

    assert len(exceptions) == 1
    assert type(exceptions[0]) is ValueError


async def test_enqueue_links_with_transform_request_function(server_url: URL) -> None:
    crawler = PlaywrightCrawler()
    visit = mock.Mock()
    headers = []

    def test_transform_request_function(request: RequestOptions) -> RequestOptions | RequestTransformAction:
        if request['url'] == str(server_url / 'sub_index'):
            request['headers'] = HttpHeaders({'transform-header': 'my-header'})
            return request
        return 'skip'

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        visit(context.request.url)
        headers.append(context.request.headers)
        await context.enqueue_links(transform_request_function=test_transform_request_function)

    await crawler.run([str(server_url / 'start_enqueue')])

    visited = {call[0][0] for call in visit.call_args_list}

    assert visited == {str(server_url / 'start_enqueue'), str(server_url / 'sub_index')}

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


async def test_redirect_handling(server_url: URL, redirect_server_url: URL) -> None:
    # Set up a dummy crawler that tracks visited URLs
    crawler = PlaywrightCrawler()
    handled_urls = set[str]()

    redirect_target = str(server_url / 'start_enqueue')
    redirect_url = str(redirect_server_url.with_path('redirect').with_query(url=redirect_target))

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        handled_urls.add(context.request.loaded_url or '')

    # Request with redirects
    request = Request.from_url(url=redirect_url)

    # Ensure that the request uses the same origin strategy - `redirect_target` will be considered out of scope
    request.crawlee_data.enqueue_strategy = 'same-origin'

    # No URLs should be visited in the run
    await crawler.run([request])
    assert handled_urls == set()


@pytest.mark.parametrize(
    'fingerprint_generator',
    [
        pytest.param(None, id='No fingerprint generator. Headers generated by header generator.'),
        pytest.param(
            DefaultFingerprintGenerator(header_options=HeaderGeneratorOptions(browsers=['chrome'])),
            id='Explicitly passed fingerprint generator.',
        ),
        pytest.param('default', id='Default fingerprint generator.'),
    ],
)
async def test_chromium_headless_headers(
    header_network: dict, fingerprint_generator: None | FingerprintGenerator | Literal['default'], server_url: URL
) -> None:
    browser_type: BrowserType = 'chromium'
    crawler = PlaywrightCrawler(headless=True, browser_type=browser_type, fingerprint_generator=fingerprint_generator)
    headers = dict[str, str]()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        response_headers = json.loads(response)

        for key, val in response_headers.items():
            headers[key] = val

    await crawler.run([str(server_url / 'headers')])

    user_agent = headers.get('user-agent')
    assert user_agent in get_available_header_values(header_network, {'user-agent', 'User-Agent'}), user_agent
    assert any(
        keyword in user_agent
        for keyword in BROWSER_TYPE_HEADER_KEYWORD[fingerprint_browser_type_from_playwright_browser_type(browser_type)]
    ), user_agent

    assert headers.get('sec-ch-ua') in get_available_header_values(header_network, 'sec-ch-ua')
    assert headers.get('sec-ch-ua-mobile') in get_available_header_values(header_network, 'sec-ch-ua-mobile')
    assert headers.get('sec-ch-ua-platform') in get_available_header_values(header_network, 'sec-ch-ua-platform')

    assert 'headless' not in headers['sec-ch-ua'].lower()
    assert 'headless' not in headers['user-agent'].lower()


async def test_firefox_headless_headers(header_network: dict, server_url: URL) -> None:
    browser_type: BrowserType = 'firefox'
    crawler = PlaywrightCrawler(headless=True, browser_type=browser_type)
    headers = dict[str, str]()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        response_headers = json.loads(response)

        for key, val in response_headers.items():
            headers[key] = val

    await crawler.run([str(server_url / 'headers')])

    assert 'user-agent' in headers
    assert 'sec-ch-ua' not in headers
    assert 'sec-ch-ua-mobile' not in headers
    assert 'sec-ch-ua-platform' not in headers

    assert 'headless' not in headers['user-agent'].lower()

    user_agent = headers.get('user-agent')
    assert user_agent in get_available_header_values(header_network, {'user-agent', 'User-Agent'})
    assert any(
        keyword in user_agent
        for keyword in BROWSER_TYPE_HEADER_KEYWORD[fingerprint_browser_type_from_playwright_browser_type(browser_type)]
    )


async def test_custom_headers(server_url: URL) -> None:
    crawler = PlaywrightCrawler()
    response_headers = dict[str, str]()
    request_headers = {'Power-Header': 'ring', 'Library': 'storm', 'My-Test-Header': 'fuzz'}

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        context_response_headers = json.loads(response)
        for key, val in context_response_headers.items():
            response_headers[key] = val

    await crawler.run([Request.from_url(str(server_url / 'headers'), headers=request_headers)])

    assert response_headers.get('power-header') == request_headers['Power-Header']
    assert response_headers.get('library') == request_headers['Library']
    assert response_headers.get('my-test-header') == request_headers['My-Test-Header']


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
async def test_isolation_cookies(*, use_incognito_pages: bool, server_url: URL) -> None:
    sessions_ids: list[str] = []
    sessions: dict[str, Session] = {}
    sessions_cookies: dict[str, dict[str, str]] = {}
    response_cookies: dict[str, dict[str, str]] = {}

    crawler = PlaywrightCrawler(
        session_pool=SessionPool(max_pool_size=1),
        use_incognito_pages=use_incognito_pages,
        concurrency_settings=ConcurrencySettings(max_concurrency=1),
    )

    @crawler.router.default_handler
    async def handler(context: PlaywrightCrawlingContext) -> None:
        if not context.session:
            return

        sessions_ids.append(context.session.id)
        sessions[context.session.id] = context.session

        if context.request.unique_key not in {'1', '2'}:
            return

        response_data = json.loads(await context.response.text())
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

    assert len(response_cookies) == 2
    assert len(sessions) == 2

    assert sessions_ids[0] == sessions_ids[1]

    sessions_cookies = {
        sessions_id: {
            cookie['name']: cookie['value'] for cookie in sessions[sessions_id].cookies.get_cookies_as_dicts()
        }
        for sessions_id in sessions_ids
    }

    assert len(sessions_cookies) == 2

    cookie_session_id = sessions_ids[0]
    clean_session_id = sessions_ids[2]

    assert cookie_session_id != clean_session_id

    # When using `use_incognito_pages` there should be full cookie isolation
    if use_incognito_pages:
        # The initiated cookies must match in both the response and the session store
        assert sessions_cookies[cookie_session_id] == response_cookies[cookie_session_id] == {'a': '1'}

        # For a clean session, the cookie should not be in the sesstion store or in the response
        # This way we can be sure that no cookies are being leaked through the http client
        assert sessions_cookies[clean_session_id] == response_cookies[clean_session_id] == {}
    # Without `use_incognito_pages` we will have access to the session cookie,
    # but there will be a cookie leak via PlaywrightContext
    else:
        # The initiated cookies must match in both the response and the session store
        assert sessions_cookies[cookie_session_id] == response_cookies[cookie_session_id] == {'a': '1'}

        # PlaywrightContext makes cookies shared by all sessions that work with it.
        # So in this case a clean session contains the same cookies
        assert sessions_cookies[clean_session_id] == response_cookies[clean_session_id] == {'a': '1'}


async def test_save_cookies_after_handler_processing(server_url: URL) -> None:
    """Test that cookies are saved correctly."""
    async with SessionPool(max_pool_size=1) as session_pool:
        crawler = PlaywrightCrawler(session_pool=session_pool)

        session_ids = []

        @crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            # Simulate cookies installed from an external source in the browser
            await context.page.context.add_cookies([{'name': 'check', 'value': 'test', 'url': str(server_url)}])

            if context.session:
                session_ids.append(context.session.id)

        await crawler.run([str(server_url)])

        assert len(session_ids) == 1

        check_session = await session_pool.get_session()

        assert check_session.id == session_ids[0]
        session_cookies = {cookie['name']: cookie['value'] for cookie in check_session.cookies.get_cookies_as_dicts()}

        assert session_cookies == {'check': 'test'}


async def test_custom_fingerprint_uses_generator_options(server_url: URL) -> None:
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

    await crawler.run([str(server_url)])

    assert 'Firefox' in fingerprints['window.navigator.userAgent']
    assert fingerprints['window.navigator.userAgentData']['platform'] == 'Android'
    assert min_width <= int(fingerprints['window.screen.width']) <= max_width
    assert min_height <= int(fingerprints['window.screen.height']) <= max_height


async def test_custom_fingerprint_matches_header_user_agent(server_url: URL) -> None:
    """Test that generated fingerprint and header have matching user agent."""

    crawler = PlaywrightCrawler(headless=True, fingerprint_generator=DefaultFingerprintGenerator())
    response_headers = dict[str, str]()
    fingerprints = dict[str, str]()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        context_response_headers = dict(json.loads(response))

        response_headers['User-Agent'] = context_response_headers['user-agent']
        fingerprints['window.navigator.userAgent'] = await context.page.evaluate('()=>window.navigator.userAgent')

    await crawler.run([str(server_url / 'headers')])

    assert response_headers['User-Agent'] == fingerprints['window.navigator.userAgent']


async def test_ignore_http_error_status_codes(server_url: URL) -> None:
    """Test that error codes that would normally trigger session error can be ignored."""
    crawler = PlaywrightCrawler(ignore_http_error_status_codes={403})
    target_url = str(server_url / 'status/403')
    mocked_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        mocked_handler(context.request.url)

    await crawler.run([target_url])

    mocked_handler.assert_called_once_with(target_url)


async def test_additional_http_error_status_codes(server_url: URL) -> None:
    """Test that use of `additional_http_error_status_codes` can raise error on common status code."""
    crawler = PlaywrightCrawler(additional_http_error_status_codes={200})

    mocked_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        mocked_handler(context.request.url)

    await crawler.run([str(server_url)])

    mocked_handler.assert_not_called()


async def test_launch_with_user_data_dir(tmp_path: Path, server_url: URL) -> None:
    """Check that the persist context is created in the specified folder in `user_data_dir`."""
    check_path = tmp_path / 'Default'
    crawler = PlaywrightCrawler(
        headless=True, user_data_dir=tmp_path, request_handler=mock.AsyncMock(return_value=None)
    )

    assert not check_path.exists()

    await crawler.run([str(server_url)])

    assert check_path.exists()


async def test_launch_with_user_data_dir_and_fingerprint(tmp_path: Path, server_url: URL) -> None:
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

    assert not check_path.exists()

    await crawler.run([str(server_url)])

    assert check_path.exists()

    assert fingerprints['window.navigator.userAgent']
    assert 'headless' not in fingerprints['window.navigator.userAgent'].lower()


async def test_get_snapshot(server_url: URL) -> None:
    crawler = PlaywrightCrawler()

    snapshot = None

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        nonlocal snapshot
        snapshot = await context.get_snapshot()

    await crawler.run([str(server_url)])

    assert snapshot is not None
    assert snapshot.html is not None
    assert snapshot.screenshot is not None
    # Check at least jpeg start and end expected bytes. Content is not relevant for the test.
    assert snapshot.screenshot.startswith(b'\xff\xd8')
    assert snapshot.screenshot.endswith(b'\xff\xd9')
    assert snapshot.html == HELLO_WORLD.decode('utf-8')


async def test_error_snapshot_through_statistics(server_url: URL) -> None:
    """Test correct use of error snapshotter by the Playwright crawler.

    In this test the crawler will visit 4 pages.
    - 2 x page endpoints will return the same error
    - homepage endpoint will return unique error
    - headers endpoint will return no error
    """
    max_retries = 2
    crawler = PlaywrightCrawler(
        statistics=Statistics.with_default_state(save_error_snapshots=True), max_request_retries=max_retries
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        if 'page' in context.request.url:
            raise RuntimeError('page error')
        if 'headers' in context.request.url:
            return
        raise RuntimeError('home error')

    await crawler.run(
        [str(server_url), str(server_url / 'page_1'), str(server_url / 'page_2'), str(server_url / 'headers')]
    )

    kvs = await crawler.get_key_value_store()
    kvs_content = {}

    async for key_info in kvs.iterate_keys():
        kvs_content[key_info.key] = await kvs.get_value(key_info.key)

        assert set(key_info.key).issubset(ErrorSnapshotter.ALLOWED_CHARACTERS)
        if key_info.key.endswith('.jpg'):
            # Check at least jpeg start and end expected bytes. Content is not relevant for the test.
            assert kvs_content[key_info.key].startswith(b'\xff\xd8')
            assert kvs_content[key_info.key].endswith(b'\xff\xd9')
        elif 'page' in key_info.key:
            assert kvs_content[key_info.key] == GENERIC_RESPONSE.decode('utf-8')
        else:
            assert kvs_content[key_info.key] == HELLO_WORLD.decode('utf-8')

    # Three errors twice retried errors, but only 2 unique -> 4 (2 x (html and jpg)) artifacts expected.
    assert crawler.statistics.error_tracker.total == 3 * max_retries
    assert crawler.statistics.error_tracker.unique_error_count == 2
    assert len(list(kvs_content.keys())) == 4


async def test_respect_robots_txt(server_url: URL) -> None:
    crawler = PlaywrightCrawler(respect_robots_txt_file=True)
    visit = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        visit(context.request.url)
        await context.enqueue_links()

    await crawler.run([str(server_url / 'start_enqueue')])
    visited = {call[0][0] for call in visit.call_args_list}

    assert visited == {
        str(server_url / 'start_enqueue'),
        str(server_url / 'sub_index'),
    }


async def test_on_skipped_request(server_url: URL) -> None:
    crawler = PlaywrightCrawler(respect_robots_txt_file=True)
    skip = mock.Mock()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
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


async def test_send_request(server_url: URL) -> None:
    """Check that the persist context works with fingerprints."""
    check_data: dict[str, Any] = {}

    crawler = PlaywrightCrawler()

    @crawler.pre_navigation_hook
    async def some_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        send_request_response = await context.send_request(str(server_url / 'user-agent'))
        check_data['pre_send_request'] = dict(json.loads(await send_request_response.read()))

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        check_data['default'] = dict(json.loads(response))
        send_request_response = await context.send_request(str(server_url / 'user-agent'))
        check_data['send_request'] = dict(json.loads(await send_request_response.read()))

    await crawler.run([str(server_url / 'user-agent')])

    assert check_data['default'].get('user-agent') is not None
    assert check_data['send_request'].get('user-agent') is not None
    assert check_data['pre_send_request'] == check_data['send_request']

    assert check_data['default'] == check_data['send_request']


async def test_send_request_with_client(server_url: URL) -> None:
    """Check that the persist context works with fingerprints."""
    check_data: dict[str, Any] = {}

    crawler = PlaywrightCrawler(
        http_client=HttpxHttpClient(header_generator=None, headers={'user-agent': 'My User-Agent'})
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        response = await context.response.text()
        check_data['default'] = dict(json.loads(response))
        send_request_response = await context.send_request(str(server_url / 'user-agent'))
        check_data['send_request'] = dict(json.loads(await send_request_response.read()))

    await crawler.run([str(server_url / 'user-agent')])

    assert check_data['default'].get('user-agent') is not None
    assert check_data['send_request']['user-agent'] == 'My User-Agent'

    assert check_data['default'] != check_data['send_request']


async def test_overwrite_configuration() -> None:
    """Check that the configuration is allowed to be passed to the Playwrightcrawler."""
    configuration = Configuration(log_level='WARNING')
    PlaywrightCrawler(configuration=configuration)
    used_configuration = service_locator.get_configuration()
    assert used_configuration is configuration


async def test_extract_links(server_url: URL) -> None:
    crawler = PlaywrightCrawler()
    extracted_links: list[str] = []

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        links = await context.extract_links(exclude=[Glob(f'{server_url}sub_index')])
        extracted_links.extend(request.url for request in links)

    await crawler.run([str(server_url / 'start_enqueue')])

    assert len(extracted_links) == 1
    assert extracted_links[0] == str(server_url / 'page_1')
