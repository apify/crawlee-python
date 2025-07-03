from collections.abc import Iterable

import pytest
from browserforge.headers import Browser

from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)
from crawlee.fingerprint_suite._browserforge_adapter import PatchedHeaderGenerator
from crawlee.fingerprint_suite._consts import BROWSER_TYPE_HEADER_KEYWORD


def test_fingerprint_generator_has_default() -> None:
    """Test that header generator can work without any options."""
    assert DefaultFingerprintGenerator().generate()


def test_fingerprint_generator_some_options_stress_test() -> None:
    """Test that header generator can work consistently."""
    fingerprint_generator = DefaultFingerprintGenerator(
        mock_web_rtc=True,
        screen_options=ScreenOptions(min_width=500),
        header_options=HeaderGeneratorOptions(strict=True),
    )

    for _ in range(20):
        fingerprint = fingerprint_generator.generate()

        assert fingerprint.mockWebRTC is True
        assert fingerprint.screen.availWidth > 500


def test_fingerprint_generator_all_options() -> None:
    """Test that header generator can work with all the options. Some most basic checks of fingerprint.

    Fingerprint generation option might have no effect if there is no fingerprint sample present in collected data.
    """
    min_width = 600
    max_width = 1800
    min_height = 400
    max_height = 1200

    fingerprint = DefaultFingerprintGenerator(
        mock_web_rtc=True,
        slim=True,
        screen_options=ScreenOptions(
            min_width=min_width,
            max_width=max_width,
            min_height=min_height,
            max_height=max_height,
        ),
        header_options=HeaderGeneratorOptions(
            strict=True,
            browsers=['firefox'],
            operating_systems=['windows'],
            devices=['mobile'],
            locales=['en'],  #  Does not generate any other values than `en-US` regardless of the input in browserforge
            http_version='2',  # Http1 does not work in browserforge
        ),
    ).generate()

    assert fingerprint.screen.availWidth >= min_width
    assert fingerprint.screen.availWidth <= max_width
    assert fingerprint.screen.availHeight >= min_height
    assert fingerprint.screen.availHeight <= max_height

    assert fingerprint.mockWebRTC is True
    assert fingerprint.slim is True
    assert 'Firefox' in fingerprint.navigator.userAgent
    assert 'Win' in fingerprint.navigator.oscpu
    assert 'en-US' in fingerprint.navigator.languages


@pytest.mark.parametrize(
    'browser',
    [
        'firefox',
        ['firefox'],
        [Browser(name='firefox')],
    ],
)
def test_patched_header_generator_generate(browser: Iterable[str | Browser]) -> None:
    """Test that PatchedHeaderGenerator works with all the possible types correctly."""
    header = PatchedHeaderGenerator().generate(browser=browser)
    assert any(keyword in header['User-Agent'] for keyword in BROWSER_TYPE_HEADER_KEYWORD['firefox'])
