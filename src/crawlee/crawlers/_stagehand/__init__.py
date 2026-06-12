from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'StagehandCrawler'):
    from ._stagehand_crawler import StagehandCrawler
with _try_import(
    __name__, 'StagehandCrawlingContext', 'StagehandPostNavCrawlingContext', 'StagehandPreNavCrawlingContext'
):
    from ._stagehand_crawling_context import (
        StagehandCrawlingContext,
        StagehandPostNavCrawlingContext,
        StagehandPreNavCrawlingContext,
    )


__all__ = [
    'StagehandCrawler',
    'StagehandCrawlingContext',
    'StagehandPostNavCrawlingContext',
    'StagehandPreNavCrawlingContext',
]
