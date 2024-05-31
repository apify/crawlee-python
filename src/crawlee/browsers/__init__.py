try:
    from .browser_pool import BrowserPool
    from .playwright_browser_plugin import PlaywrightBrowserPlugin
except ImportError as exc:
    raise ImportError(
        'To use this module, you need to install the "playwright" extra. Run "pip install crawlee[playwright]".',
    ) from exc
