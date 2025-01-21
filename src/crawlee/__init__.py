from importlib import metadata

from ._request import Request, RequestOptions
from ._service_locator import service_locator
from ._types import ConcurrencySettings, EnqueueStrategy, HttpHeaders
from ._utils.globs import Glob

__version__ = metadata.version('crawlee')

__all__ = [
    'ConcurrencySettings',
    'EnqueueStrategy',
    'Glob',
    'HttpHeaders',
    'Request',
    'RequestOptions',
    'service_locator',
]
