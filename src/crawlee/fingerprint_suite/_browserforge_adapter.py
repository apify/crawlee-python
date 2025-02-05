from __future__ import annotations

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
    from camoufox.utils import ListOrString

    from ._types import HeaderGeneratorOptions, ScreenOptions, SupportedBrowserType


class PatchedHeaderGenerator(bf_HeaderGenerator):
    """Browserforge `HeaderGenerator` that contains patches not accepted in upstream repo."""

    def _get_accept_language_header(self, locales: ListOrString) -> str:
        """Generates the Accept-Language header based on the given locales.

        Patched version due to PR of upstream repo not being merged: https://github.com/daijro/browserforge/pull/24

        Parameters:
            locales (ListOrString): Locale(s).

        Returns:
            str: Accept-Language header string.
        """
        # First locale does not include quality factor, q=1 is considered as implicit.
        additional_locales = [f'{locale};q={0.9 - index * 0.1:.1f}' for index, locale in enumerate(locales[1:])]
        return ','.join((locales[0], *additional_locales))


class PatchedFingerprintGenerator(bf_FingerprintGenerator):
    """Browserforge `FingerprintGenerator` that contains patches not accepted in upstream repo."""

    def __init__(  # type:ignore[no-untyped-def]  # Upstream repo types missing.
        self,
        *,
        screen: Screen | None = None,
        strict: bool = False,
        mock_webrtc: bool = False,
        slim: bool = False,
        **header_kwargs,  # noqa:ANN003 # Upstream repo types missing.
    ) -> None:
        """Initializes the FingerprintGenerator with the given options.

        Parameters:
            screen (Screen, optional): Screen constraints for the generated fingerprint.
            strict (bool, optional): Whether to raise an exception if the constraints are too strict. Default is False.
            mock_webrtc (bool, optional): Whether to mock WebRTC when injecting the fingerprint. Default is False.
            slim (bool, optional): Disables performance-heavy evasions when injecting the fingerprint. Default is False.
            **header_kwargs: Header generation options for HeaderGenerator
        """
        super().__init__(screen=screen, strict=strict, mock_webrtc=mock_webrtc, slim=slim)
        # Replace `self.header_generator` To make sure that we consistently use `PatchedHeaderGenerator`
        self.header_generator = PatchedHeaderGenerator(**header_kwargs)


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
        self._generator = PatchedFingerprintGenerator()

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


class BrowserforgeHeaderGenerator:
    """`HeaderGenerator` adapter for fingerprint generator from `browserforge`."""

    def __init__(self) -> None:
        self._generator = PatchedHeaderGenerator(locale=['en-US', 'en'])

    def generate(self, browser_type: SupportedBrowserType = 'chromium') -> dict[str, str]:
        # browserforge header generation can be flaky. Enforce basic QA on generated headers
        max_attempts = 10

        bf_browser_type = 'safari' if browser_type == 'webkit' else browser_type

        for _attempt in range(max_attempts):
            generated_header: dict[str, str] = self._generator.generate(browser=bf_browser_type)
            if any(keyword in generated_header['User-Agent'] for keyword in BROWSER_TYPE_HEADER_KEYWORD[browser_type]):
                return generated_header
        raise RuntimeError('Failed to generate header.')


def get_available_header_network() -> dict:
    """Get header network that contains possible header values."""
    return extract_json(DATA_DIR / 'header-network.zip')


def get_available_header_values(header_network: dict, node_name: str | set[str]) -> set[str]:
    """Get set of possible header values from available header network."""
    node_names = {node_name} if isinstance(node_name, str) else node_name
    for node in header_network['nodes']:
        if node['name'] in node_names:
            return set(node['possibleValues'])
    return set()
