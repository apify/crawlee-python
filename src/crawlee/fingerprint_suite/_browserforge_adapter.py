from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from browserforge.bayesian_network import extract_json
from browserforge.fingerprints import Fingerprint as bf_Fingerprint
from browserforge.fingerprints import FingerprintGenerator as bf_FingerprintGenerator
from browserforge.fingerprints import Screen
from browserforge.headers.generator import DATA_DIR
from browserforge.headers.generator import HeaderGenerator as bf_HeaderGenerator
from typing_extensions import override

from crawlee._utils.docs import docs_group

from ._consts import BROWSER_TYPE_HEADER_KEYWORD
from ._fingerprint_generator import FingerprintGenerator

if TYPE_CHECKING:
    from ._types import HeaderGeneratorOptions, ScreenOptions, SupportedBrowserType


@docs_group('Classes')
class BrowserforgeFingerprintGenerator(FingerprintGenerator):
    """`FingerprintGenerator` adapter for fingerprint generator from `browserforge`."""

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
        self._generator = bf_FingerprintGenerator()

    @override
    def generate(self) -> bf_Fingerprint:
        # browserforge fingerprint generation can be flaky
        # https://github.com/daijro/browserforge/issues/22"
        # During test runs around 10 % flakiness was detected.
        # Max attempt set to 10 as (0.1)^10 is considered sufficiently low probability.
        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                return self._generator.generate(**self._options)
            except ValueError:  # noqa:PERF203
                if attempt == max_attempts:
                    raise
        raise RuntimeError('Failed to generate fingerprint.')


@docs_group('Classes')
class BrowserforgeHeaderGenerator:

    def __init__(self):
        self._generator = bf_HeaderGenerator()

    def generate(self, browser_type: SupportedBrowserType = "chromium") -> bf_Fingerprint:
        # browserforge header generation can be flaky. Enforce basic QA on generated headers
        max_attempts = 10

        if browser_type=='webkit':
            bf_browser_type = 'safari'
        else:
            bf_browser_type = browser_type

        for attempt in range(max_attempts):
            generated_header = self._generator.generate(browser=bf_browser_type)
            if any(keyword in generated_header['User-Agent'] for keyword in BROWSER_TYPE_HEADER_KEYWORD[browser_type]):
                return generated_header
            print(generated_header['User-Agent'])
        raise RuntimeError('Failed to generate header.')



def get_user_agent_pool() -> set[str]:
    """Get set of `User-Agent` strings available to browserforge."""
    header_network = extract_json((DATA_DIR / "header-network.zip"))
    return set(header_network['nodes'][6]['possibleValues']) | set(header_network['nodes'][7]['possibleValues'])

