from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

from ._base import BaseHttpClient, HttpCrawlingResult, HttpResponse
from ._httpx import HttpxHttpClient

_install_import_hook(__name__)

# The following imports use try_import to handle optional dependencies, as they may not always be available.

with _try_import(__name__, 'CurlImpersonateHttpClient'):
    from ._curl_impersonate import CurlImpersonateHttpClient


__all__ = [
    'BaseHttpClient',
    'CurlImpersonateHttpClient',
    'HttpCrawlingResult',
    'HttpResponse',
    'HttpxHttpClient',
]
