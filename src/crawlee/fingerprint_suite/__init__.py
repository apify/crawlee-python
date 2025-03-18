# ruff: noqa: E402, TID252

# Due to patch_browserforge
from .._browserforge_workaround import patch_browserforge

patch_browserforge()

from ._browserforge_adapter import BrowserforgeFingerprintGenerator as DefaultFingerprintGenerator
from ._fingerprint_generator import FingerprintGenerator
from ._header_generator import HeaderGenerator
from ._types import HeaderGeneratorOptions, ScreenOptions

__all__ = [
    'DefaultFingerprintGenerator',
    'FingerprintGenerator',
    'HeaderGenerator',
    'HeaderGeneratorOptions',
    'ScreenOptions',
]
