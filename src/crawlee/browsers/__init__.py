from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

from ._types import BrowserType, CrawleePage

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

with _try_import(__name__, 'StagehandBrowserController'):
    from ._stagehand_browser_controller import StagehandBrowserController
with _try_import(__name__, 'StagehandBrowserPlugin'):
    from ._stagehand_browser_plugin import StagehandBrowserPlugin
with _try_import(__name__, 'StagehandOptions', 'StagehandPage'):
    from ._stagehand_types import StagehandOptions, StagehandPage


__all__ = [
    'BrowserPool',
    'BrowserType',
    'CrawleePage',
    'PlaywrightBrowserController',
    'PlaywrightBrowserPlugin',
    'PlaywrightPersistentBrowser',
    'StagehandBrowserController',
    'StagehandBrowserPlugin',
    'StagehandOptions',
    'StagehandPage',
]
