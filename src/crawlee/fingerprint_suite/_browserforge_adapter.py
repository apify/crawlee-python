"""Input and ouput adapter for camoufox fingerprint handling"""
from browserforge.fingerprints import Fingerprint as bf_Fingerprint, FingerprintGenerator as bf_FingerprintGenerator
from typing_extensions import override

from crawlee.fingerprint_suite._fingerprint_generator import AbstractFingerprintGenerator
from crawlee.fingerprint_suite._types import Fingerprint, FingerprintGeneratorOptions

class FingerprintGenerator(AbstractFingerprintGenerator):

    @staticmethod
    def get_fingerprint(bf_fingerprint: bf_Fingerprint) -> Fingerprint:
        return Fingerprint.model_validate(bf_fingerprint, from_attributes=True)

    @override
    @staticmethod
    def generate(options: FingerprintGeneratorOptions | None = None) -> Fingerprint:
        bf_fingerprint = bf_FingerprintGenerator().generate(**(options or {}))
        return Fingerprint.model_validate(bf_fingerprint, from_attributes=True)

