from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies (the `ai` extra),
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'AiCrawler'):
    from ._ai_crawler import AiCrawler
with _try_import(__name__, 'AiCrawlingContext'):
    from ._ai_crawling_context import AiCrawlingContext
with _try_import(__name__, 'BaseAiHtmlExtractor'):
    from ._base_extractor import BaseAiHtmlExtractor
with _try_import(__name__, 'AiDirectExtractor'):
    from ._direct_extractor import AiDirectExtractor
with _try_import(__name__, 'AiSelectorExtractor'):
    from ._selector_extractor import AiSelectorExtractor
with _try_import(__name__, 'BaseAiHtmlDistiller'):
    from ._base_distiller import BaseAiHtmlDistiller
with _try_import(__name__, 'AiCleanHtmlDistiller'):
    from ._clean_html_distiller import AiCleanHtmlDistiller
with _try_import(__name__, 'AiSkeletonDistiller'):
    from ._skeleton_distiller import AiSkeletonDistiller
with _try_import(__name__, 'AiHtmlDistiller', 'AiHtmlExtractor', 'AiUsageStats'):
    from ._types import AiHtmlDistiller, AiHtmlExtractor, AiUsageStats
with _try_import(__name__, 'get_basic_ai_cleaner'):
    from ._utils import get_basic_ai_cleaner

__all__ = [
    'AiCleanHtmlDistiller',
    'AiCrawler',
    'AiCrawlingContext',
    'AiDirectExtractor',
    'AiHtmlDistiller',
    'AiHtmlExtractor',
    'AiSelectorExtractor',
    'AiSkeletonDistiller',
    'AiUsageStats',
    'BaseAiHtmlDistiller',
    'BaseAiHtmlExtractor',
    'get_basic_ai_cleaner',
]
