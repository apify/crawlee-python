from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any

from browserforge.fingerprints import Fingerprint as bf_Fingerprint
from browserforge.fingerprints import FingerprintGenerator as bf_FingerprintGenerator
from browserforge.fingerprints import Screen
from typing_extensions import override

from ._fingerprint_generator import FingerprintGenerator

if TYPE_CHECKING:
    from ._types import HeaderGeneratorOptions, ScreenOptions


class BrowserforgeFingerprintGenerator(FingerprintGenerator):
    def __init__(
        self,
        *,
        header_options: HeaderGeneratorOptions | None = None,
        screen_options: ScreenOptions | None = None,
        mock_web_rtc: bool | None = None,
        slim: bool | None = None,
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
        bf_options: dict[str, Any] = {'mock_webrtc': mock_web_rtc, 'slim': slim}

        if header_options is None:
            bf_header_options = {}
        else:
            bf_header_options = deepcopy(header_options.model_dump())
            bf_header_options['browser'] = bf_header_options.pop('browsers', None)
            bf_header_options['os'] = bf_header_options.pop('operating_systems', None)
            bf_header_options['device'] = bf_header_options.pop('devices', None)
            bf_header_options['locale'] = bf_header_options.pop('locales', None)

        if screen_options is None:
            bf_options['screen'] = Screen()
        else:
            bf_options['screen'] = Screen(**screen_options.model_dump())

        self._options = {**bf_options, **bf_header_options}

    @override
    def generate(self) -> bf_Fingerprint:
        return bf_FingerprintGenerator().generate(**self._options)