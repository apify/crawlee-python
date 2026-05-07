from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from playwright.async_api import Browser, Playwright, async_playwright
from stagehand import AsyncSession, AsyncStagehand

from crawlee.browsers import StagehandBrowserController, StagehandOptions, StagehandPage
from crawlee.proxy_configuration import ProxyInfo

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from yarl import URL


@pytest.fixture
async def playwright() -> AsyncGenerator[Playwright, None]:
    async with async_playwright() as playwright:
        yield playwright


@pytest.fixture
async def browser(playwright: Playwright) -> AsyncGenerator[Browser, None]:
    browser = await playwright.chromium.launch()
    # Stagehand's CDP setup always provides a pre-existing context; simulate it.
    await browser.new_context()
    yield browser
    await browser.close()


@pytest.fixture
def session_mock() -> MagicMock:
    session = MagicMock(spec=AsyncSession)
    session.data = MagicMock()
    session.data.cdp_url = 'ws://fake-cdp'
    session.end = AsyncMock()
    return session


@pytest.fixture
def stagehand_client_mock(session_mock: MagicMock) -> MagicMock:
    client = MagicMock(spec=AsyncStagehand)
    client.sessions.start = AsyncMock(return_value=session_mock)
    return client


@pytest.fixture
async def controller(
    playwright: Playwright,
    browser: Browser,
    stagehand_client_mock: MagicMock,
) -> AsyncGenerator[StagehandBrowserController, None]:
    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client_mock,
        stagehand_options=StagehandOptions(),
        max_open_pages_per_browser=2,
    )
    with patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)):
        yield controller
    await controller.close(force=True)


async def test_initial_state(playwright: Playwright, session_mock: MagicMock) -> None:
    stagehand_client = MagicMock()
    stagehand_client.sessions.start = AsyncMock(return_value=session_mock)

    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client,
        stagehand_options=StagehandOptions(),
    )

    assert controller.pages == []
    assert controller.pages_count == 0
    assert isinstance(controller.last_page_opened_at, datetime)
    assert controller.idle_time < timedelta(seconds=1)
    assert controller.has_free_capacity
    assert controller.is_browser_connected  # True before any session is started
    assert controller.browser_type == 'chromium'


async def test_open_and_close_page(controller: StagehandBrowserController) -> None:
    page = await controller.new_page()

    assert isinstance(page, StagehandPage)
    assert page in controller.pages
    assert controller.pages_count == 1
    assert controller.last_page_opened_at <= datetime.now(timezone.utc)

    await page.close()

    assert page not in controller.pages
    assert controller.pages_count == 0


async def test_max_open_pages_limit(controller: StagehandBrowserController) -> None:
    page1 = await controller.new_page()
    assert controller.pages_count == 1

    page2 = await controller.new_page()
    assert controller.pages_count == 2

    with pytest.raises(ValueError, match=r'Cannot open more pages in this browser.'):
        await controller.new_page()

    assert controller.pages_count == 2

    await page1.close()
    assert controller.pages_count == 1

    page3 = await controller.new_page()
    assert controller.pages_count == 2

    await page2.close()
    await page3.close()

    assert controller.pages == []
    assert controller.pages_count == 0


async def test_idle_time(controller: StagehandBrowserController) -> None:
    idle_time_before = controller.idle_time
    await asyncio.sleep(1)  # Simulate waiting
    idle_time_after = controller.idle_time
    assert idle_time_after > idle_time_before


async def test_close_browser_with_open_pages(
    playwright: Playwright,
    browser: Browser,
    stagehand_client_mock: MagicMock,
    session_mock: MagicMock,
) -> None:
    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client_mock,
        stagehand_options=StagehandOptions(),
    )

    with patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)):
        _ = await controller.new_page()

        with pytest.raises(ValueError, match=r'Cannot close the browser while there are open pages.'):
            await controller.close()

        assert controller.pages_count == 1
        assert controller.is_browser_connected

        await controller.close(force=True)

        assert controller.pages_count == 0

    assert session_mock.end.await_count == 1
    assert not controller.is_browser_connected


async def test_second_page_reuses_session(
    controller: StagehandBrowserController,
    stagehand_client_mock: MagicMock,
) -> None:
    page1 = await controller.new_page()
    page2 = await controller.new_page()

    assert stagehand_client_mock.sessions.start.await_count == 1
    assert page1.context == page2.context
    assert page1 is not page2  # Different Page instances

    await page1.close()
    await page2.close()


async def test_concurrent_session_init(
    playwright: Playwright,
    browser: Browser,
    session_mock: MagicMock,
) -> None:
    call_count = 0

    async def delayed_start(**_kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(5)  # Simulate delay in session start
        return session_mock

    stagehand_client = MagicMock()
    stagehand_client.sessions.start = delayed_start

    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client,
        stagehand_options=StagehandOptions(),
        max_open_pages_per_browser=5,
    )

    with patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)):
        pages = await asyncio.gather(controller.new_page(), controller.new_page())

        # Only one session should be started despite concurrent calls to `new_page()`.
        assert call_count == 1

        for page in pages:
            await page.close()
        await controller.close(force=True)


async def test_session_start_params_local(
    playwright: Playwright,
    browser: Browser,
    stagehand_client_mock: MagicMock,
) -> None:

    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client_mock,
        stagehand_options=StagehandOptions(
            env='LOCAL',
            model='openai/gpt-4o',
            verbose=1,
            self_heal=False,
            dom_settle_timeout_ms=500.0,
            system_prompt='Test prompt',
        ),
        max_open_pages_per_browser=2,
    )

    with patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)):
        page = await controller.new_page()
        await page.close()
        await controller.close()

    call_kwargs = stagehand_client_mock.sessions.start.call_args.kwargs
    assert call_kwargs['model_name'] == 'openai/gpt-4o'
    assert call_kwargs['verbose'] == 1
    assert call_kwargs['self_heal'] is False
    assert call_kwargs['dom_settle_timeout_ms'] == 500.0
    assert call_kwargs['system_prompt'] == 'Test prompt'
    assert call_kwargs['browser'] == {'type': 'local', 'launch_options': {}}


async def test_session_start_params_browserbase(
    playwright: Playwright,
    browser: Browser,
    stagehand_client_mock: MagicMock,
) -> None:
    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client_mock,
        stagehand_options=StagehandOptions(env='BROWSERBASE'),
        max_open_pages_per_browser=2,
    )

    with patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)):
        page = await controller.new_page()
        await page.close()
        await controller.close()

    call_kwargs = stagehand_client_mock.sessions.start.call_args.kwargs
    assert call_kwargs['browser'] == {'type': 'browserbase', 'launch_options': {}}
    assert 'browserbase_session_create_params' not in call_kwargs


async def test_proxy_set_local(
    playwright: Playwright,
    browser: Browser,
    session_mock: MagicMock,
) -> None:
    stagehand_client = MagicMock()
    stagehand_client.sessions.start = AsyncMock(return_value=session_mock)

    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client,
        stagehand_options=StagehandOptions(env='LOCAL'),
        max_open_pages_per_browser=2,
    )

    proxy = ProxyInfo(
        url='http://proxy.example.com:8080',
        scheme='http',
        hostname='proxy.example.com',
        port=8080,
        username='user',
        password='pass',
    )

    with patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)):
        await controller.new_page(proxy_info=proxy)
        await controller.close(force=True)

    call_kwargs = stagehand_client.sessions.start.call_args.kwargs

    browser_options = call_kwargs['browser']
    assert browser_options['type'] == 'local'

    proxy_options = browser_options['launch_options']['proxy']
    assert proxy_options['server'] == 'http://proxy.example.com:8080'
    assert proxy_options['username'] == 'user'
    assert proxy_options['password'] == 'pass'


async def test_proxy_set_browserbase(
    playwright: Playwright,
    browser: Browser,
    session_mock: MagicMock,
) -> None:
    stagehand_client = MagicMock()
    stagehand_client.sessions.start = AsyncMock(return_value=session_mock)

    controller = StagehandBrowserController(
        playwright=playwright,
        stagehand_client=stagehand_client,
        stagehand_options=StagehandOptions(env='BROWSERBASE'),
        max_open_pages_per_browser=2,
    )

    proxy = ProxyInfo(
        url='http://proxy.example.com:8080',
        scheme='http',
        hostname='proxy.example.com',
        port=8080,
        username='user',
        password='pass',
    )

    with patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)):
        await controller.new_page(proxy_info=proxy)
        await controller.close(force=True)

    call_kwargs = stagehand_client.sessions.start.call_args.kwargs

    browserbase_proxy_options = call_kwargs['browserbase_session_create_params']
    assert browserbase_proxy_options['proxies'][0]['type'] == 'external'
    assert browserbase_proxy_options['proxies'][0]['server'] == 'http://proxy.example.com:8080'
    assert browserbase_proxy_options['proxies'][0]['username'] == 'user'
    assert browserbase_proxy_options['proxies'][0]['password'] == 'pass'


async def test_fingerprint_headers_set_on_new_page(controller: StagehandBrowserController, server_url: URL) -> None:
    page = await controller.new_page()

    response = await page.goto(str(server_url / 'headers'))

    assert response is not None

    response_json = await response.json()

    assert 'Headless' not in response_json['user-agent']
