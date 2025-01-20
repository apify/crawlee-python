from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from browserforge.fingerprints import Fingerprint

    from crawlee.fingerprint_suite._types import HeaderGeneratorOptions, ScreenOptions


class AbstractFingerprintGenerator(ABC):
    @abstractmethod
    def __init__(
        self,
        *,
        header_options: HeaderGeneratorOptions | None = None,
        screen_options: ScreenOptions | None = None,
        mock_web_rtc: bool | None = None,
        slim: bool | None = None,
        strict: bool = False,
    ) -> None:
        """A default constructor.

        All generator options are optional. If any value is not specified, then `None` is set in the options.
        Default values for options set to `None` are implementation detail of used fingerprint generator.
        Specific default values should not be relied upon. Use explicit values if it matters for your use case.

        Args:
            header_options: Collection of header related attributes that can be used by the fingerprint generator.
            screen_options: Defines the screen constrains for the fingerprint generator.
            mock_web_rtc: Whether to mock WebRTC when injecting the fingerprint.
            slim: Disables performance-heavy evasions when injecting the fingerprint.
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
