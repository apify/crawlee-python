from importlib import metadata

from ._models import Request
from ._types import ConcurrencySettings, EnqueueStrategy
from ._utils.globs import Glob

__version__ = metadata.version('crawlee')
