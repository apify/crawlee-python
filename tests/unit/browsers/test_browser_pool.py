from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest

from crawlee.browsers import BrowserPool, PlaywrightBrowserPlugin
from crawlee.browsers._browser_controller import BrowserController
from crawlee.browsers._types import CrawleePage
from tests.unit.utils import run_alone_on_mac

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any

    from yarl import URL

    from crawlee.browsers._browser_plugin import BrowserPlugin
    from crawlee.proxy_configuration import ProxyInfo


async def test_default_plugin_new_page_creation(server_url: URL) -> None:
    async with BrowserPool() as browser_pool:
        page_1 = await browser_pool.new_page()
        await page_1.page.goto(str(server_url))
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == str(server_url)
        assert '<html' in await page_1.page.content()  # there is some HTML content
        assert browser_pool.total_pages_count == 1

        page_2 = await browser_pool.new_page()
        await page_2.page.goto(str(server_url / 'status/200'))
        assert page_2.browser_type == 'chromium'
        assert page_2.page.url == str(server_url / 'status/200')
        assert '<html' in await page_1.page.content()  # there is some HTML content
        assert browser_pool.total_pages_count == 2

        await page_1.page.close()
        await page_2.page.close()


async def test_multiple_plugins_new_page_creation(server_url: URL) -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        assert browser_pool.plugins == [plugin_chromium, plugin_firefox]

        page_1 = await browser_pool.new_page()
        await page_1.page.goto(str(server_url))
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == str(server_url)
        assert '<html' in await page_1.page.content()  # there is some HTML content

        page_2 = await browser_pool.new_page()
        await page_2.page.goto(str(server_url / 'headers'))
        assert page_2.browser_type == 'firefox'
        assert page_2.page.url == str(server_url / 'headers')
        assert '<html' in await page_2.page.content()  # there is some HTML content

        page_3 = await browser_pool.new_page()
        await page_3.page.goto(str(server_url / 'user-agent'))
        assert page_3.browser_type == 'chromium'
        assert page_3.page.url == str(server_url / 'user-agent')
        assert '<html' in await page_3.page.content()  # there is some HTML content

        await page_1.page.close()
        await page_2.page.close()
        await page_3.page.close()

        assert browser_pool.total_pages_count == 3


@pytest.mark.flaky(
    rerun=3,
    reason='Test is flaky on Windows and MacOS, see https://github.com/apify/crawlee-python/issues/1660.',
)
async def test_new_page_with_each_plugin(server_url: URL) -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        pages = await browser_pool.new_page_with_each_plugin()

        assert len(pages) == 2

        assert pages[0].browser_type == 'chromium'
        assert pages[1].browser_type == 'firefox'

        await pages[0].page.goto(str(server_url))
        assert pages[0].page.url == str(server_url)
        assert '<html' in await pages[0].page.content()  # there is some HTML content

        await pages[1].page.goto(str(server_url / 'headers'))
        assert pages[1].page.url == str(server_url / 'headers')
        assert '<html' in await pages[1].page.content()

        for page in pages:
            await page.page.close()

        assert browser_pool.total_pages_count == 2


@run_alone_on_mac
async def test_with_default_plugin_constructor(server_url: URL) -> None:
    # Use a generous operation timeout so that Firefox has enough time to launch on slow Windows CI.
    async with BrowserPool.with_default_plugin(
        headless=True, browser_type='firefox', operation_timeout=timedelta(seconds=60)
    ) as browser_pool:
        assert len(browser_pool.plugins) == 1
        assert isinstance(browser_pool.plugins[0], PlaywrightBrowserPlugin)

        page = await browser_pool.new_page()
        assert page.browser_type == 'firefox'

        await page.page.goto(str(server_url))
        assert page.page.url == str(server_url)
        assert '<html' in await page.page.content()  # there is some HTML content

        await page.page.close()
        assert browser_pool.total_pages_count == 1


async def test_new_page_with_existing_id() -> None:
    async with BrowserPool() as browser_pool:
        page_1 = await browser_pool.new_page()
        with pytest.raises(ValueError, match=r'Page with ID: .* already exists.'):
            await browser_pool.new_page(page_id=page_1.id)


async def test_new_page_with_invalid_plugin() -> None:
    plugin_1 = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_2 = PlaywrightBrowserPlugin(browser_type='firefox')
    async with BrowserPool([plugin_1]) as browser_pool:
        with pytest.raises(ValueError, match=r'Provided browser_plugin is not one of the plugins used by BrowserPool.'):
            await browser_pool.new_page(browser_plugin=plugin_2)


async def test_resource_management(server_url: URL) -> None:
    playwright_plugin = PlaywrightBrowserPlugin(browser_type='chromium')

    async with BrowserPool([playwright_plugin]) as browser_pool:
        page = await browser_pool.new_page()
        await page.page.goto(str(server_url))
        assert page.page.url == str(server_url)
        assert '<html' in await page.page.content()  # there is some HTML content
        assert browser_pool.total_pages_count == 1

    # All pages should be closed in __aexit__
    assert page.page.is_closed()


async def test_methods_raise_error_when_not_active() -> None:
    plugin = PlaywrightBrowserPlugin()
    browser_pool = BrowserPool([plugin])

    assert browser_pool.active is False

    with pytest.raises(RuntimeError, match=r'BrowserPool is not active.'):
        await browser_pool.new_page()

    with pytest.raises(RuntimeError, match=r'BrowserPool is not active.'):
        await browser_pool.new_page_with_each_plugin()

    with pytest.raises(RuntimeError, match=r'BrowserPool is already active.'):
        async with browser_pool, browser_pool:
            pass

    async with browser_pool:
        assert browser_pool.active is True


async def test_with_plugin_contains_page_options(server_url: URL) -> None:
    plugin = PlaywrightBrowserPlugin(browser_new_context_options={'user_agent': 'My Best User-Agent'})
    async with BrowserPool(plugins=[plugin]) as browser_pool:
        test_page = await browser_pool.new_page()
        await test_page.page.goto(str(server_url / 'user-agent'))
        assert 'My Best User-Agent' in await test_page.page.content()
        await test_page.page.close()


@pytest.mark.parametrize(
    ('retire_after_page_count', 'expect_equal_browsers'),
    [
        pytest.param(2, True, id='Two pages opened in the same browser'),
        pytest.param(1, False, id='Each page opened in a new browser.'),
    ],
)
async def test_browser_pool_retire_browser_after_page_count(
    retire_after_page_count: int, *, expect_equal_browsers: bool
) -> None:
    async with BrowserPool(retire_browser_after_page_count=retire_after_page_count) as browser_pool:
        test_page = await browser_pool.new_page()
        first_browser = test_page.page.context
        await test_page.page.close()

        test_page = await browser_pool.new_page()
        second_browser = test_page.page.context

        await test_page.page.close()

        if expect_equal_browsers:
            assert first_browser is second_browser
        else:
            assert first_browser is not second_browser


async def test_pre_page_create_hook_is_called() -> None:
    call_mock = AsyncMock()

    async with BrowserPool() as browser_pool:

        @browser_pool.pre_page_create_hook
        async def hook(
            page_id: str,
            controller: BrowserController,
            browser_new_context_options: dict[str, Any],
            proxy_info: ProxyInfo | None,
        ) -> None:
            await call_mock(page_id, controller, browser_new_context_options, proxy_info)

            browser_new_context_options['user_agent'] = 'Modified User-Agent'

            assert len(controller.pages) == 0

        test_page = await browser_pool.new_page()
        user_agent = await test_page.page.evaluate('navigator.userAgent')

        await test_page.page.close()

    assert user_agent == 'Modified User-Agent'

    call_mock.assert_awaited_once()
    page_id, controller, _, proxy_info = call_mock.call_args[0]

    assert isinstance(page_id, str)
    assert test_page.id == page_id
    assert isinstance(controller, BrowserController)
    assert proxy_info is None


async def test_post_page_create_hook_is_called() -> None:
    call_mock = AsyncMock()

    async with BrowserPool() as browser_pool:

        @browser_pool.post_page_create_hook
        async def hook(crawlee_page: CrawleePage, controller: BrowserController) -> None:
            await call_mock(crawlee_page, controller)
            await crawlee_page.page.evaluate('window.__hook_applied = true')

            assert isinstance(crawlee_page, CrawleePage)

            assert len(controller.pages) == 1

        test_page = await browser_pool.new_page()

        js_result = await test_page.page.evaluate('window.__hook_applied')

        await test_page.page.close()

    assert js_result is True

    call_mock.assert_awaited_once()
    crawlee_page, controller = call_mock.call_args[0]

    assert test_page is crawlee_page
    assert isinstance(controller, BrowserController)


async def test_pre_page_close_hook() -> None:
    call_mock = AsyncMock()

    async with BrowserPool() as browser_pool:

        @browser_pool.pre_page_close_hook
        async def hook(crawlee_page: CrawleePage, controller: BrowserController) -> None:
            await call_mock(crawlee_page, controller)

            assert not crawlee_page.page.is_closed()
            assert len(controller.pages) == 1

        test_page = await browser_pool.new_page()
        await test_page.page.close()

    call_mock.assert_awaited_once()
    assert test_page.page.is_closed()


async def test_post_page_close_hook() -> None:
    call_mock = AsyncMock()

    async with BrowserPool() as browser_pool:

        @browser_pool.post_page_close_hook
        async def hook(page_id: str, controller: BrowserController) -> None:
            await call_mock(page_id, controller)

            assert len(controller.pages) == 0

        test_page = await browser_pool.new_page()
        await test_page.page.close()

    page_id, controller = call_mock.call_args[0]

    call_mock.assert_awaited_once()
    assert test_page.id == page_id
    assert isinstance(controller, BrowserController)


async def test_hooks_execution_order() -> None:
    call_order: list[str] = []

    async with BrowserPool() as browser_pool:

        @browser_pool.pre_launch_hook
        async def pre_launch(_page_id: str, _plugin: BrowserPlugin) -> None:
            call_order.append('pre_launch')

        @browser_pool.post_launch_hook
        async def post_launch(_page_id: str, _controller: BrowserController) -> None:
            call_order.append('post_launch')

        @browser_pool.pre_page_create_hook
        async def pre_create(
            _page_id: str,
            _controller: BrowserController,
            _browser_new_context_options: Mapping[str, Any],
            _proxy_info: ProxyInfo | None,
        ) -> None:
            call_order.append('pre_create')

        @browser_pool.post_page_create_hook
        async def post_create(_crawlee_page: CrawleePage, _controller: BrowserController) -> None:
            call_order.append('post_create')

        @browser_pool.pre_page_close_hook
        async def pre_close(_crawlee_page: CrawleePage, _controller: BrowserController) -> None:
            call_order.append('pre_close')

        @browser_pool.post_page_close_hook
        async def post_close(_page_id: str, _controller: BrowserController) -> None:
            call_order.append('post_close')

        page = await browser_pool.new_page()
        await page.page.close()

    assert call_order == ['pre_launch', 'post_launch', 'pre_create', 'post_create', 'pre_close', 'post_close']


async def test_multiple_hooks_all_called() -> None:
    call_order: list[str] = []

    async with BrowserPool() as browser_pool:

        @browser_pool.post_page_create_hook
        async def first(_crawlee_page: CrawleePage, _controller: BrowserController) -> None:
            call_order.append('first')

        @browser_pool.post_page_create_hook
        async def second(_crawlee_page: CrawleePage, _controller: BrowserController) -> None:
            call_order.append('second')

        page = await browser_pool.new_page()
        await page.page.close()

    assert call_order == ['first', 'second']


async def test_pre_launch_hook_is_called() -> None:
    call_mock = AsyncMock()

    async with BrowserPool() as browser_pool:

        @browser_pool.pre_launch_hook
        async def hook(page_id: str, plugin: BrowserPlugin) -> None:
            await call_mock(page_id, plugin)

        test_page = await browser_pool.new_page()
        await test_page.page.close()

    call_mock.assert_awaited_once()
    page_id, plugin = call_mock.call_args[0]

    assert isinstance(page_id, str)
    assert test_page.id == page_id
    assert isinstance(plugin, PlaywrightBrowserPlugin)


async def test_post_launch_hook_is_called() -> None:
    call_mock = AsyncMock()

    async with BrowserPool() as browser_pool:

        @browser_pool.post_launch_hook
        async def hook(page_id: str, controller: BrowserController) -> None:
            await call_mock(page_id, controller)

        test_page = await browser_pool.new_page()
        await test_page.page.close()

    call_mock.assert_awaited_once()
    page_id, controller = call_mock.call_args[0]

    assert isinstance(page_id, str)
    assert test_page.id == page_id
    assert isinstance(controller, BrowserController)


async def test_post_launch_hook_error_closes_browser() -> None:
    async with BrowserPool() as browser_pool:

        @browser_pool.post_launch_hook
        async def hook(_page_id: str, _controller: BrowserController) -> None:
            raise ValueError('Hook failed')

        with pytest.raises(ValueError, match='Hook failed'):
            await browser_pool.new_page()

        assert len(browser_pool.active_browsers) == 0
        assert len(browser_pool.inactive_browsers) == 0


async def test_launch_hooks_not_called_for_existing_browser() -> None:
    launch_hook_calls = 0

    async with BrowserPool() as browser_pool:

        @browser_pool.pre_launch_hook
        async def hook(_page_id: str, _plugin: BrowserPlugin) -> None:
            nonlocal launch_hook_calls
            launch_hook_calls += 1

        page_1 = await browser_pool.new_page()
        page_2 = await browser_pool.new_page()

        await page_1.page.close()
        await page_2.page.close()

    assert launch_hook_calls == 1
