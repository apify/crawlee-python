from playwright.async_api import async_playwright
from yarl import URL

from crawlee.crawlers._playwright._utils import block_requests, infinite_scroll


async def test_infinite_scroll_on_dynamic_page(server_url: URL) -> None:
    """Checks that infinite_scroll loads all items on a page with infinite scrolling."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        target_url = str(server_url / 'infinite_scroll')

        # Get data with manual scrolling
        await page.goto(target_url)

        manual_items = []
        for _ in range(4):
            items = await page.query_selector_all('.item')
            manual_items = items
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(1000)

        # Reset page
        await page.close()
        page = await browser.new_page()
        await page.goto(target_url)

        # Get data with infinite_scroll utility
        before_scroll = await page.query_selector_all('.item')
        assert len(before_scroll) != len(manual_items)
        assert len(before_scroll) == 10

        await infinite_scroll(page)

        after_scroll = await page.query_selector_all('.item')

        assert len(before_scroll) < len(after_scroll)
        assert len(manual_items) == len(after_scroll)

        await browser.close()


async def test_infinite_scroll_no_page_without_scroll(server_url: URL) -> None:
    """Checks that infinite_scroll does not call error on a page without infinite scrolling."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(str(server_url))

        await infinite_scroll(page)

        title = await page.title()

        assert title == 'Hello, world!'

        await browser.close()


async def test_double_call_infinite_scroll(server_url: URL) -> None:
    """Checks that calling infinite_scroll twice does not load more items the second time."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(str(server_url / 'infinite_scroll'))

        await infinite_scroll(page)
        first_count = len(await page.query_selector_all('.item'))

        await infinite_scroll(page)
        second_count = len(await page.query_selector_all('.item'))

        assert first_count == second_count

        await browser.close()


async def test_block_requests_default(server_url: URL) -> None:
    """Checks that block_requests blocks the correct resources by default."""
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        target_url = str(server_url / 'resource_loading_page')

        # Default behavior, all resources load
        page = await browser.new_page()
        loaded_urls_no_block = []

        page.on('requestfinished', lambda req: loaded_urls_no_block.append(req.url.rsplit('/', 1)[-1]))
        await page.goto(target_url)
        await page.wait_for_load_state('networkidle')
        await page.close()

        # With blocking — collect loaded resources
        page = await browser.new_page()
        loaded_urls_blocked = []

        page.on('requestfinished', lambda req: loaded_urls_blocked.append(req.url.rsplit('/', 1)[-1]))
        await block_requests(page)
        await page.goto(target_url)
        await page.wait_for_load_state('networkidle')
        await page.close()

        await browser.close()

    # Without blocking, both resources should load
    assert set(loaded_urls_no_block) == {'resource_loading_page', 'test.js', 'test.png'}

    # With blocking, only JS should load
    assert set(loaded_urls_blocked) == {'resource_loading_page', 'test.js'}


async def test_block_requests_with_extra_patterns(server_url: URL) -> None:
    """Checks that block_requests blocks the correct resources with extra patterns."""
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        target_url = str(server_url / 'resource_loading_page')

        page = await browser.new_page()
        loaded_urls_blocked = []

        page.on('requestfinished', lambda req: loaded_urls_blocked.append(req.url.rsplit('/', 1)[-1]))
        await block_requests(page, extra_url_patterns=['*.js'])
        await page.goto(target_url)
        await page.wait_for_load_state('networkidle')
        await page.close()

        await browser.close()

        # With blocking, only HTML should load
        assert set(loaded_urls_blocked) == {'resource_loading_page'}


async def test_block_requests_with_custom_patterns(server_url: URL) -> None:
    """Checks that block_requests blocks the correct resources with custom patterns."""
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        target_url = str(server_url / 'resource_loading_page')

        page = await browser.new_page()
        loaded_urls_blocked = []

        page.on('requestfinished', lambda req: loaded_urls_blocked.append(req.url.rsplit('/', 1)[-1]))
        await block_requests(page, url_patterns=['*.js'])
        await page.goto(target_url)
        await page.wait_for_load_state('networkidle')
        await page.close()

        await browser.close()

        # With blocking, only PNG should load
        assert set(loaded_urls_blocked) == {'resource_loading_page', 'test.png'}
