from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from browserforge.fingerprints import Fingerprint


class FingerprintGenerator(ABC):
    @abstractmethod
    def generate(self) -> Fingerprint:
        """Method that is capable of generating fingerprints.

        This is experimental feature.
        Return type is temporarily set to `Fingerprint` from `browserforge`. This is subject to change and most likely
        it will change to custom `Fingerprint` class defined in this repo later.
        """
        ...
