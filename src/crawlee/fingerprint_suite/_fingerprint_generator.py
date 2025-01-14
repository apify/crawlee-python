from abc import ABC, abstractmethod

from browserforge.fingerprints import Fingerprint

from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions


class AbstractFingerprintGenerator(ABC):

    @abstractmethod
    def __init__(self, fingerprint_generator_options: FingerprintGeneratorOptions, strict: bool = False):
        """A default constructor.

        Args:
            fingerprint_generator_options: Options used for generating fingerprints.
            strict: If set to True, it will raise error if it is not possible to generate fingerprints based on the
                fingerprint_generator_options. Default behavior is relaxation of fingerprint_generator_options until it
                is possible to generate a fingerprint.
        """
        ...

    @abstractmethod
    def generate(self) -> Fingerprint:
        """Method that is capable of generating fingerprints.

        If generator needs some settings or arguments, then it is expected to be done in `init`.

        This is experimental feature.
        Return type is temporarily set to `Fingerprint` from `browserforge`. This is subject to change and most likely
        it will change to custom `Fingerprint` class defined in this repo later.
        """
        ...

