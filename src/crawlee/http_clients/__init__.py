from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

# These imports have only mandatory dependencies, so they are imported directly.
from ._base import HttpClient, HttpCrawlingResult, HttpResponse
from ._httpx import HttpxHttpClient

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'CurlImpersonateHttpClient'):
    from ._curl_impersonate import CurlImpersonateHttpClient


__all__ = [
    'CurlImpersonateHttpClient',
    'HttpClient',
    'HttpCrawlingResult',
    'HttpResponse',
    'HttpxHttpClient',
]
