from copy import deepcopy

from browserforge.fingerprints import Fingerprint as bf_Fingerprint, FingerprintGenerator as bf_FingerprintGenerator, \
    Screen
from typing_extensions import override

from crawlee.fingerprint_suite._fingerprint_generator import AbstractFingerprintGenerator
from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions

class FingerprintGenerator(AbstractFingerprintGenerator):

    def __init__(self, fingerprint_generator_options: FingerprintGeneratorOptions | None = None, strict: bool = False):
        self._fingerprint_generator_options = FingerprintGenerator._prepare_options(
            fingerprint_generator_options or FingerprintGeneratorOptions())
        self._strict = strict

    @override
    def generate(self) -> bf_Fingerprint:
        bf_fingerprint = bf_FingerprintGenerator().generate(**self._fingerprint_generator_options)
        return bf_fingerprint

    @staticmethod
    def _prepare_options(options: FingerprintGeneratorOptions) -> dict[any,any]:
        """Adapt options for `browserforge.fingerprints.FingerprintGenerator`."""
        raw_options = options.model_dump()
        bf_options = {}
        if raw_options["header_options"] is None:
            bf_header_options = dict()
        else:
            bf_header_options = deepcopy(raw_options["header_options"])
            bf_header_options["browser"] = bf_header_options.pop("browsers", None)
            bf_header_options["os"] = bf_header_options.pop("operating_systems", None)
            bf_header_options["device"] = bf_header_options.pop("devices", None)
            bf_header_options["locale"] = bf_header_options.pop("locales", None)

        bf_options["mock_webrtc"] = raw_options["mock_web_rtc"]
        bf_options["screen"] = Screen(**(raw_options.get("screen") or {}))
        bf_options["slim"] = raw_options["slim"]
        return {**bf_options , **bf_header_options}
