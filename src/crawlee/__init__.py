import logging
from importlib import metadata

from ._request import Request, RequestOptions, RequestState
from ._service_locator import service_locator
from ._types import ConcurrencySettings, EnqueueStrategy, HttpHeaders, RequestTransformAction, SkippedReason
from ._utils.globs import Glob

__version__ = metadata.version('crawlee')

# Keeps Python from printing Crawlee's warnings through `logging.lastResort` when the application has not configured
# logging.
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    'ConcurrencySettings',
    'EnqueueStrategy',
    'Glob',
    'HttpHeaders',
    'Request',
    'RequestOptions',
    'RequestState',
    'RequestTransformAction',
    'SkippedReason',
    'service_locator',
]
