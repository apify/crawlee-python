try:
    from ._browser_pool import BrowserPool
    from ._playwright_browser_controller import PlaywrightBrowserController
    from ._playwright_browser_plugin import PlaywrightBrowserPlugin
except ImportError as exc:
    raise ImportError(
        "To import anything from this subpackage, you need to install the 'playwright' extra."
        "For example, if you use pip, run `pip install 'crawlee[playwright]'`.",
    ) from exc

__all__ = ['BrowserPool', 'PlaywrightBrowserController', 'PlaywrightBrowserPlugin']
