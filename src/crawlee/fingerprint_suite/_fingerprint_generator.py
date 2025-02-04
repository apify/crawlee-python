from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from browserforge.fingerprints import Fingerprint


@docs_group('Abstract classes')
class FingerprintGenerator(ABC):
    """A class for creating browser fingerprints that mimic browser fingerprints of real users."""

    @abstractmethod
    def generate(self) -> Fingerprint:
        """Generate browser fingerprints.

        This is experimental feature.
        Return type is temporarily set to `Fingerprint` from `browserforge`. This is subject to change and most likely
        it will change to custom `Fingerprint` class defined in this repo later.
        """
