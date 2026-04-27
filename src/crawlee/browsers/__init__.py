from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

from ._types import BrowserType, CrawleePage, StagehandOptions, StagehandPage

_install_import_hook(__name__)


# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'BrowserPool'):
    from ._browser_pool import BrowserPool
with _try_import(__name__, 'PlaywrightBrowserController'):
    from ._playwright_browser_controller import PlaywrightBrowserController
with _try_import(__name__, 'PlaywrightBrowserPlugin'):
    from ._playwright_browser_plugin import PlaywrightBrowserPlugin
with _try_import(__name__, 'PlaywrightPersistentBrowser'):
    from ._playwright_browser import PlaywrightPersistentBrowser


__all__ = [
    'BrowserPool',
    'BrowserType',
    'CrawleePage',
    'PlaywrightBrowserController',
    'PlaywrightBrowserPlugin',
    'PlaywrightPersistentBrowser',
    'StagehandOptions',
    'StagehandPage',
]
