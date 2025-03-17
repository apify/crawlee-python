from __future__ import annotations

import os.path
from collections.abc import Iterable
from copy import deepcopy
from functools import reduce
from operator import or_
from typing import TYPE_CHECKING, Any, Literal

from browserforge.bayesian_network import extract_json
from browserforge.fingerprints import Fingerprint as bf_Fingerprint
from browserforge.fingerprints import FingerprintGenerator as bf_FingerprintGenerator
from browserforge.fingerprints import Screen
from browserforge.headers import Browser
from browserforge.headers.generator import DATA_DIR, ListOrString
from browserforge.headers.generator import HeaderGenerator as bf_HeaderGenerator
from typing_extensions import override

from crawlee._utils.docs import docs_group

from ._consts import BROWSER_TYPE_HEADER_KEYWORD
from ._fingerprint_generator import FingerprintGenerator

if TYPE_CHECKING:
    from ._types import HeaderGeneratorOptions, ScreenOptions, SupportedBrowserType


class PatchedHeaderGenerator(bf_HeaderGenerator):
    """Browserforge `HeaderGenerator` that contains patches specific for our usage of the generator."""

    def _get_accept_language_header(self, locales: tuple[str, ...]) -> str:
        """Generates the Accept-Language header based on the given locales.

        Patched version due to PR of upstream repo not being merged: https://github.com/daijro/browserforge/pull/24

        Args:
            locales: Locale(s).

        Returns:
            Accept-Language header string.
        """
        # First locale does not include quality factor, q=1 is considered as implicit.
        additional_locales = [f'{locale};q={0.9 - index * 0.1:.1f}' for index, locale in enumerate(locales[1:])]
        return ','.join((locales[0], *additional_locales))

    def generate(
        self,
        *,
        browser: Iterable[str | Browser] | None = None,
        os: ListOrString | None = None,
        device: ListOrString | None = None,
        locale: ListOrString | None = None,
        http_version: Literal[1, 2] | None = None,
        user_agent: ListOrString | None = None,
        strict: bool | None = None,
        request_dependent_headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Generate HTTP headers based on the specified parameters.

        For detailed description of the original method see: `browserforge.headers.generator.HeaderGenerator.generate`
        This patched version of the method adds additional quality checks on the output of the original method. It tries
        to generate headers several times until they match the requirements.

        The `browser` parameter accepts `chromium` as a general category, which includes not only Google Chrome
        but also other Chromium-based browsers. As a result, a Safari-like user agent may be generated for a `chromium`
        input, such as:
        ```
        Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko)
         CriOS/130.0.6723.90 Mobile/15E148 Safari/604.1
        ```
        To maintain consistency with previous implementations, only a subset of Chromium headers will be allowed.

        Returns:
            A generated headers.
        """
        # browserforge header generation can be flaky. Enforce basic QA on generated headers
        max_attempts = 10

        single_browser = self._get_single_browser_type(browser)

        if single_browser == 'chromium':
            # `BrowserForge` header generator considers `chromium` in general sense and therefore will generate also
            # other `Chromium` based browser headers. This adapter desires only specific subset of `chromium` headers
            # that contain all 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform' headers.
            # Increase max attempts as from `BrowserForge` header generator perspective even `chromium`
            # headers without `sec-...` headers are valid.
            max_attempts += 50

        # Browserforge uses term 'safari', we use term 'webkit'
        bf_browser_type = 'safari' if single_browser == 'webkit' else single_browser

        # Use browserforge to generate headers until it satisfies our additional requirements.
        for _attempt in range(max_attempts):
            generated_header: dict[str, str] = super().generate(
                browser=bf_browser_type,
                os=os,
                device=device,
                locale=locale,
                http_version=http_version,
                user_agent=user_agent,
                strict=strict,
                request_dependent_headers=request_dependent_headers,
            )

            if ('headless' in generated_header.get('User-Agent', '').lower()) or (
                'headless' in generated_header.get('sec-ch-ua', '').lower()
            ):
                # It can be a valid header, but we never want to leak "headless". Get a different one.
                continue

            if any(
                keyword in generated_header['User-Agent']
                for keyword in self._get_expected_browser_keywords(single_browser)
            ):
                if single_browser == 'chromium' and not self._contains_all_sec_headers(generated_header):
                    # Accept chromium header only with all sec headers.
                    continue

                return generated_header
        raise RuntimeError('Failed to generate header.')

    def _contains_all_sec_headers(self, headers: dict[str, str]) -> bool:
        return all(header_name in headers for header_name in ('sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform'))

    def _get_expected_browser_keywords(self, browser: str | None) -> set[str]:
        if not browser:
            # Allow all possible keywords when there is no preference for specific browser type.
            return reduce(or_, BROWSER_TYPE_HEADER_KEYWORD.values())

        return BROWSER_TYPE_HEADER_KEYWORD[browser]

    def _get_single_browser_type(self, browser: Iterable[str | Browser] | None) -> str | None:
        """Get single browser type.

        Browserforge header generator accepts wider range of possible types.
        Narrow it to single optional string as that is how we use it.
        Handling the original multitype would be pointlessly complex.
        """
        # In our case we never pass more than one browser type. In general case more browsers are just bigger pool to
        # select from, so narrowing it to the first one is still a valid action.
        first_browser = (
            next(iter(browser)) if (isinstance(browser, Iterable) and not isinstance(browser, str)) else browser
        )

        if isinstance(first_browser, str):
            single_name = first_browser
        elif isinstance(first_browser, Browser):
            single_name = first_browser.name
        else:
            single_name = None

        return single_name


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
        """A default constructor.

        Args:
            screen: Screen constraints for the generated fingerprint.
            strict: Whether to raise an exception if the constraints are too strict.
            mock_webrtc: Whether to mock WebRTC when injecting the fingerprint.
            slim: Disables performance-heavy evasions when injecting the fingerprint.
            **header_kwargs: Header generation options for `HeaderGenerator`.
        """
        super().__init__(screen=screen, strict=strict, mock_webrtc=mock_webrtc, slim=slim)
        # Replace `self.header_generator` To make sure that we consistently use `PatchedHeaderGenerator`
        self.header_generator = PatchedHeaderGenerator(**header_kwargs)


@docs_group('Classes')
class BrowserforgeFingerprintGenerator(FingerprintGenerator):
    """`FingerprintGenerator` adapter for fingerprint generator from `browserforge`.

    `browserforge` is a browser header and fingerprint generator: https://github.com/daijro/browserforge
    """

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
        """Generate headers."""
        return self._generator.generate(browser=browser_type)


def get_available_header_network() -> dict:
    """Get header network that contains possible header values."""
    if os.path.isfile(DATA_DIR / 'header-network.zip'):
        return extract_json(DATA_DIR / 'header-network.zip')
    if os.path.isfile(DATA_DIR / 'header-network-definition.zip'):
        return extract_json(DATA_DIR / 'header-network-definition.zip')
    raise FileNotFoundError('Missing header-network file.')


def get_available_header_values(header_network: dict, node_name: str | set[str]) -> set[str]:
    """Get set of possible header values from available header network."""
    node_names = {node_name} if isinstance(node_name, str) else node_name
    for node in header_network['nodes']:
        if node['name'] in node_names:
            return set(node['possibleValues'])
    return set()
