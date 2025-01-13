from abc import ABC

from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions, Fingerprint


class AbstractFingerprintGenerator(ABC):

    @staticmethod
    def generate(options: FingerprintGeneratorOptions | None = None) -> Fingerprint:
        ...

