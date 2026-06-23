from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies (the `pydantic-ai` extra),
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'PydanticAiCrawler'):
    from ._pydantic_ai_crawler import PydanticAiCrawler
with _try_import(__name__, 'PydanticAiCrawlingContext'):
    from ._pydantic_ai_crawling_context import PydanticAiCrawlingContext
with _try_import(__name__, 'BasePydanticAiHtmlExtractor'):
    from ._base_extractor import BasePydanticAiHtmlExtractor
with _try_import(__name__, 'PydanticAiDirectExtractor'):
    from ._direct_extractor import PydanticAiDirectExtractor
with _try_import(__name__, 'PydanticAiSelectorExtractor'):
    from ._selector_extractor import PydanticAiSelectorExtractor
with _try_import(__name__, 'BasePydanticAiHtmlDistiller'):
    from ._base_distiller import BasePydanticAiHtmlDistiller
with _try_import(__name__, 'PydanticAiCleanHtmlDistiller'):
    from ._clean_html_distiller import PydanticAiCleanHtmlDistiller
with _try_import(__name__, 'PydanticAiSkeletonDistiller'):
    from ._skeleton_distiller import PydanticAiSkeletonDistiller
with _try_import(__name__, 'PydanticAiHtmlDistiller', 'PydanticAiHtmlExtractor', 'PydanticAiUsageStats'):
    from ._types import PydanticAiHtmlDistiller, PydanticAiHtmlExtractor, PydanticAiUsageStats
with _try_import(__name__, 'get_basic_http_cleaner'):
    from ._utils import get_basic_http_cleaner

__all__ = [
    'BasePydanticAiHtmlDistiller',
    'BasePydanticAiHtmlExtractor',
    'PydanticAiCleanHtmlDistiller',
    'PydanticAiCrawler',
    'PydanticAiCrawlingContext',
    'PydanticAiDirectExtractor',
    'PydanticAiHtmlDistiller',
    'PydanticAiHtmlExtractor',
    'PydanticAiSelectorExtractor',
    'PydanticAiSkeletonDistiller',
    'PydanticAiUsageStats',
    'get_basic_http_cleaner',
]
