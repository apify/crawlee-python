from __future__ import annotations

from copy import deepcopy
from typing import Any

from browserforge.fingerprints import Fingerprint as bf_Fingerprint
from browserforge.fingerprints import FingerprintGenerator as bf_FingerprintGenerator
from browserforge.fingerprints import Screen
from typing_extensions import override

from crawlee.fingerprint_suite._fingerprint_generator import AbstractFingerprintGenerator
from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions


class FingerprintGenerator(AbstractFingerprintGenerator):
    def __init__(self, options: FingerprintGeneratorOptions | None = None, *, strict: bool = False) -> None:
        self._options = FingerprintGenerator._prepare_options(options or FingerprintGeneratorOptions())
        self._strict = strict

    @override
    def generate(self) -> bf_Fingerprint:
        return bf_FingerprintGenerator().generate(**self._options)

    @staticmethod
    def _prepare_options(options: FingerprintGeneratorOptions) -> dict[Any, Any]:
        """Adapt options for `browserforge.fingerprints.FingerprintGenerator`."""
        raw_options = options.model_dump()
        bf_options = {}
        if raw_options['header_options'] is None:
            bf_header_options = {}
        else:
            bf_header_options = deepcopy(raw_options['header_options'])
            bf_header_options['browser'] = bf_header_options.pop('browsers', None)
            bf_header_options['os'] = bf_header_options.pop('operating_systems', None)
            bf_header_options['device'] = bf_header_options.pop('devices', None)
            bf_header_options['locale'] = bf_header_options.pop('locales', None)

        bf_options['mock_webrtc'] = raw_options['mock_web_rtc']
        bf_options['screen'] = Screen(**(raw_options.get('screen') or {}))
        bf_options['slim'] = raw_options['slim']
        return {**bf_options, **bf_header_options}
