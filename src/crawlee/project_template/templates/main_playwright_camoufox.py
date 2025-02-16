# % extends 'main.py'

# % block import
from camoufox import AsyncNewBrowser
from typing_extensions import override

from crawlee._utils.context import ensure_context
from crawlee.browsers import PlaywrightBrowserPlugin, PlaywrightBrowserController, BrowserPool
from crawlee.crawlers import PlaywrightCrawler
# % endblock

# % block pre_main
class CamoufoxPlugin(PlaywrightBrowserPlugin):
    """Example browser plugin that uses Camoufox Browser, but otherwise keeps the functionality of
    PlaywrightBrowserPlugin."""

    @ensure_context
    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        return PlaywrightBrowserController(
            browser=await AsyncNewBrowser(self._playwright, headless=True),
            max_open_pages_per_browser=1,  #  Increase, if camoufox can handle it in your usecase.
            header_generator=None,  #  This turns off the crawlee header_generation. Camoufox has its own.
        )
# % endblock

# % block instantiation
crawler = PlaywrightCrawler(
    max_requests_per_crawl=10,
    request_handler=router,
    browser_pool=BrowserPool(plugins=[CamoufoxPlugin()])
)
# % endblock
