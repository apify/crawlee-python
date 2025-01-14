from abc import ABC, abstractmethod

from browserforge.fingerprints import Fingerprint

from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions


class AbstractFingerprintGenerator(ABC):
    @abstractmethod
    def __init__(self, options: FingerprintGeneratorOptions, *, strict: bool = False) -> None:
        """A default constructor.

        Args:
            options: Options used for generating fingerprints.
            strict: If set to `True`, it will raise error if it is not possible to generate fingerprints based on the
                `options`. Default behavior is relaxation of `options` until it is possible to generate a fingerprint.
        """
        ...

    @abstractmethod
    def generate(self) -> Fingerprint:
        """Method that is capable of generating fingerprints.

        This is experimental feature.
        Return type is temporarily set to `Fingerprint` from `browserforge`. This is subject to change and most likely
        it will change to custom `Fingerprint` class defined in this repo later.
        """
        ...
