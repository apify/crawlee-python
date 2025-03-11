from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

_install_import_hook(__name__)

# ruff: noqa
# Temporary fix until https://github.com/daijro/browserforge/pull/29 is merged
from pathlib import Path
from typing import Dict

import apify_fingerprint_datapoints  # type:ignore[import-untyped]
from browserforge import download
from browserforge.download import DATA_FILES

# Needed to be done before the import of code that does import time download
download.DATA_DIRS: Dict[str, Path] = {  # type:ignore[misc]
    'headers': apify_fingerprint_datapoints.get_header_network().parent,
    'fingerprints': apify_fingerprint_datapoints.get_fingerprint_network().parent,
}

import browserforge.bayesian_network


class BayesianNetwork(browserforge.bayesian_network.BayesianNetwork):
    def __init__(self, path: Path) -> None:
        if path.name in DATA_FILES['headers']:
            path = download.DATA_DIRS['headers'] / path.name
        else:
            path = download.DATA_DIRS['fingerprints'] / path.name
        super().__init__(path)


browserforge.bayesian_network.BayesianNetwork = BayesianNetwork  # type:ignore[misc]

import browserforge.headers.generator

browserforge.headers.generator.DATA_DIR = download.DATA_DIRS['headers']
import browserforge.fingerprints.generator

browserforge.headers.generator.DATA_DIR = download.DATA_DIRS['fingerprints']
# End of fix

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'BrowserPool'):
    from ._browser_pool import BrowserPool
with _try_import(__name__, 'PlaywrightBrowserController'):
    from ._playwright_browser_controller import PlaywrightBrowserController
with _try_import(__name__, 'PlaywrightBrowserPlugin'):
    from ._playwright_browser_plugin import PlaywrightBrowserPlugin

__all__ = [
    'BrowserPool',
    'PlaywrightBrowserController',
    'PlaywrightBrowserPlugin',
]
