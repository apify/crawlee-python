import asyncio

from crawlee.browsers import BrowserPool, PlaywrightBrowserController, PlaywrightBrowserPlugin
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee._utils.context import ensure_context
from typing_extensions import override


class CustomBrowserPlugin(PlaywrightBrowserPlugin):
    """A custom browser plugin that launches a browser from a custom executable path."""

    def __init__(self, executable_path: str, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._executable_path = executable_path

    @ensure_context
    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        browser = await self._playwright.chromium.launch(
            executable_path=self._executable_path,
            headless=True,
        )
        return PlaywrightBrowserController(
            browser=browser,
            max_open_pages_per_browser=self.max_open_pages_per_browser,
        )


async def main() -> None:
    plugin = CustomBrowserPlugin(executable_path='/path/to/custom/browser')
    browser_pool = BrowserPool(plugins=[plugin])
    crawler = PlaywrightCrawler(browser_pool=browser_pool)

    @crawler.router.default_handler
    async def handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Crawling: {context.request.url}')

    await crawler.run(['https://crawlee.dev'])


asyncio.run(main())