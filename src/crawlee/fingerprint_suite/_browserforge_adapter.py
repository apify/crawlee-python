from browserforge.fingerprints import Fingerprint as bf_Fingerprint, FingerprintGenerator as bf_FingerprintGenerator, \
    Screen
from typing_extensions import override

from crawlee.fingerprint_suite._fingerprint_generator import AbstractFingerprintGenerator
from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions

class FingerprintGenerator(AbstractFingerprintGenerator):

    @override
    @staticmethod
    def generate(options: FingerprintGeneratorOptions | None = None, strict: bool = False) -> bf_Fingerprint:
        options = options or FingerprintGeneratorOptions()
        bf_options = FingerprintGenerator._prepare_options(options)

        bf_fingerprint = bf_FingerprintGenerator().generate(
            screen = Screen(**(bf_options["screen"] or {})),
            mock_webrtc = bf_options["mock_web_rtc"],
            slim=bf_options["slim"],
            **bf_options["header_options"])
        return bf_fingerprint

    @staticmethod
    def _prepare_options(options: FingerprintGeneratorOptions) -> dict[any,any]:
        bf_options = options.model_dump()
        if bf_options["header_options"] is None:
            bf_options["header_options"] = dict()
        else:
            bf_options["header_options"]["browser"] = bf_options["header_options"].pop("browsers", None)
            bf_options["header_options"]["os"] = bf_options["header_options"].pop("operating_systems", None)
            bf_options["header_options"]["device"] = bf_options["header_options"].pop("devices", None)
            bf_options["header_options"]["locale"] = bf_options["header_options"].pop("locales", None)
        return bf_options
