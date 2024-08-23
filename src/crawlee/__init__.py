from importlib import metadata

from ._autoscaling.types import ConcurrencySettings
from ._models import Request
from ._utils.globs import Glob

__version__ = metadata.version('crawlee')
