from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from playwright.async_api import async_playwright
from stagehand import AsyncSession, AsyncStagehand

from crawlee.browsers import (
    BrowserPool,
    PlaywrightBrowserPlugin,
    StagehandBrowserPlugin,
    StagehandOptions,
    StagehandPage,
)
from crawlee.crawlers import (
    PlaywrightCrawlingContext,
    StagehandCrawler,
    StagehandCrawlingContext,
    StagehandPostNavCrawlingContext,
    StagehandPreNavCrawlingContext,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from yarl import URL


_PLUGIN_MODULE = 'crawlee.browsers._stagehand_browser_plugin'


@pytest.fixture
def stagehand_session_mock() -> MagicMock:
    session = MagicMock(spec=AsyncSession)
    session.data = MagicMock()
    session.data.cdp_url = 'ws://fake-cdp'
    session.end = AsyncMock()
    return session


@pytest.fixture
async def patched_crawler(stagehand_session_mock: MagicMock) -> AsyncGenerator[StagehandCrawler, None]:
    """StagehandCrawler with real Playwright but Stagehand session mocked."""
    stagehand_client = MagicMock(spec=AsyncStagehand)
    stagehand_client.sessions.start = AsyncMock(return_value=stagehand_session_mock)
    stagehand_client.__aenter__ = AsyncMock(return_value=stagehand_client)
    stagehand_client.__aexit__ = AsyncMock()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        # Stagehand's CDP always provides a pre-existing context; simulate it.
        await browser.new_context()

        mock_playwright_cm = AsyncMock()
        mock_playwright_cm.__aenter__ = AsyncMock(return_value=playwright)
        mock_playwright_cm.__aexit__ = AsyncMock()

        with (
            patch(f'{_PLUGIN_MODULE}.AsyncStagehand', return_value=stagehand_client),
            patch(f'{_PLUGIN_MODULE}.async_playwright', return_value=mock_playwright_cm),
            patch.object(playwright.chromium, 'connect_over_cdp', AsyncMock(return_value=browser)),
        ):
            yield StagehandCrawler()


def test_init_raises_with_browser_pool_and_params() -> None:
    pool = BrowserPool(plugins=[StagehandBrowserPlugin()])

    with pytest.raises(ValueError, match=r'Cannot specify'):
        StagehandCrawler(browser_pool=pool, stagehand_options=StagehandOptions())

    with pytest.raises(ValueError, match=r'Cannot specify'):
        StagehandCrawler(browser_pool=pool, headless=True)

    with pytest.raises(ValueError, match=r'Cannot specify'):
        StagehandCrawler(browser_pool=pool, browser_launch_options={'headless': True})


def test_init_raises_with_mixed_plugins_in_browser_pool() -> None:
    pool = BrowserPool(plugins=[StagehandBrowserPlugin(), PlaywrightBrowserPlugin()])

    with pytest.raises(ValueError, match=r'All BrowserPool plugins must be StagehandBrowserPlugin instances'):
        StagehandCrawler(browser_pool=pool)


def test_init_with_valid_browser_pool() -> None:
    pool = BrowserPool(plugins=[StagehandBrowserPlugin()])
    StagehandCrawler(browser_pool=pool)


async def test_context_type(patched_crawler: StagehandCrawler, server_url: URL) -> None:
    handler_mock = AsyncMock()
    pre_navigation_hook_mock = AsyncMock()
    post_navigation_hook_mock = AsyncMock()
    patched_crawler.pre_navigation_hook(pre_navigation_hook_mock)
    patched_crawler.post_navigation_hook(post_navigation_hook_mock)
    patched_crawler.router.default_handler(handler_mock)

    await patched_crawler.run([str(server_url)])

    context = handler_mock.call_args[0][0]
    assert isinstance(context, StagehandCrawlingContext)
    assert isinstance(context, PlaywrightCrawlingContext)
    assert isinstance(context.page, StagehandPage)


async def test_pre_navigation_hook(patched_crawler: StagehandCrawler, server_url: URL) -> None:
    pre_nav_hook_mock = AsyncMock()
    patched_crawler.pre_navigation_hook(pre_nav_hook_mock)
    patched_crawler.router.default_handler(AsyncMock())

    await patched_crawler.run([str(server_url)])

    pre_nav_hook_mock.assert_called_once()
    context = pre_nav_hook_mock.call_args[0][0]
    assert isinstance(context, StagehandPreNavCrawlingContext)
    assert isinstance(context.page, StagehandPage)


async def test_post_navigation_hook(patched_crawler: StagehandCrawler, server_url: URL) -> None:
    post_nav_hook_mock = AsyncMock()
    patched_crawler.post_navigation_hook(post_nav_hook_mock)
    patched_crawler.router.default_handler(AsyncMock())

    await patched_crawler.run([str(server_url)])

    post_nav_hook_mock.assert_called_once()
    context = post_nav_hook_mock.call_args[0][0]
    assert isinstance(context, StagehandPostNavCrawlingContext)
    assert isinstance(context.page, StagehandPage)


async def test_stagehand_page_ai_methods_delegate_to_session(
    patched_crawler: StagehandCrawler,
    stagehand_session_mock: MagicMock,
    server_url: URL,
) -> None:
    @patched_crawler.router.default_handler
    async def handler(context: StagehandCrawlingContext) -> None:
        await context.page.act(input='click button')
        await context.page.extract(instruction='get title')
        await context.page.observe(instruction='find links')
        await context.page.execute(agent_config={}, execute_options={'instruction': 'run script'})

    await patched_crawler.run([str(server_url)])

    for method_name, argument in (
        ('act', 'input'),
        ('extract', 'instruction'),
        ('observe', 'instruction'),
        ('execute', 'execute_options'),
    ):
        method_mock = getattr(stagehand_session_mock, method_name)
        method_mock.assert_awaited_once()
        assert isinstance(method_mock.call_args.kwargs['page'], StagehandPage)

        assert argument in method_mock.call_args.kwargs
