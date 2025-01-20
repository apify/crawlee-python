import pytest

from crawlee.fingerprint_suite import (
    AbstractFingerprintGenerator,
    DefaultFingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)


@pytest.mark.parametrize('FingerprintGenerator', [pytest.param(DefaultFingerprintGenerator, id='browserforge')])
def test_fingerprint_generator_has_default(FingerprintGenerator: type[AbstractFingerprintGenerator]) -> None:  # noqa:N803  # Test is more readable if argument(class) is PascalCase
    """Test that header generator can work without any options."""
    assert FingerprintGenerator().generate()


@pytest.mark.parametrize('FingerprintGenerator', [pytest.param(DefaultFingerprintGenerator, id='browserforge')])
def test_fingerprint_generator_some_options(FingerprintGenerator: type[AbstractFingerprintGenerator]) -> None:  # noqa:N803  # Test is more readable if argument(class) is PascalCase
    """Test that header generator can work with only some options."""

    fingerprint = FingerprintGenerator(
        mock_web_rtc=True,
        screen_options=ScreenOptions(min_width=500),
        header_options=HeaderGeneratorOptions(strict=True),
    ).generate()

    assert fingerprint.mockWebRTC is True
    assert fingerprint.screen.availWidth >= 500


@pytest.mark.parametrize('FingerprintGenerator', [pytest.param(DefaultFingerprintGenerator, id='browserforge')])
def test_fingerprint_generator_all_options(FingerprintGenerator: type[AbstractFingerprintGenerator]) -> None:  # noqa:N803  # Test is more readable if argument(class) is PascalCase
    """Test that header generator can work with all the options. Some most basic checks of fingerprint.

    Fingerprint generation option might have no effect if there is no fingerprint sample present in collected data.
    """
    min_width = 600
    max_width = 1800
    min_height = 400
    max_height = 1200

    fingerprint = FingerprintGenerator(
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
