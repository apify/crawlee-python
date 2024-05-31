try:
    from .browser_pool import BrowserPool
    from .playwright_browser_plugin import PlaywrightBrowserPlugin
except ImportError as exc:
    raise ImportError(
        'To import anything from this subpacakge, you need to install the "playwright" extra. '
        'For example, if you use pip, run "pip install crawlee[playwright]".',
    ) from exc
