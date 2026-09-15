from importlib import metadata

from ._request import Request, RequestOptions, RequestState
from ._service_locator import service_locator
from ._types import ConcurrencySettings, EnqueueStrategy, HttpHeaders, RequestTransformAction, SkippedReason
from ._utils.globs import Glob

# isort: split
# The snapshotter imports `service_locator` from `crawlee`, so the autoscaling package has to come after it.
from ._autoscaling import AutoscaledPool, ThroughputAutoscaledPool

__version__ = metadata.version('crawlee')

__all__ = [
    'AutoscaledPool',
    'ConcurrencySettings',
    'EnqueueStrategy',
    'Glob',
    'HttpHeaders',
    'Request',
    'RequestOptions',
    'RequestState',
    'RequestTransformAction',
    'SkippedReason',
    'ThroughputAutoscaledPool',
    'service_locator',
]
