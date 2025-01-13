import pytest

from crawlee.fingerprint_suite._browserforge_adapter import FingerprintGenerator as bf_FingerprintGenerator
from crawlee.fingerprint_suite._fingerprint_generator import AbstractFingerprintGenerator
from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions, ScreenOptions, HeaderGeneratorOptions


@pytest.mark.parametrize("fingerprint_generator",[
                pytest.param(bf_FingerprintGenerator, id="Browserforge"),
])
def test_fingerprint_generator_has_default(fingerprint_generator: AbstractFingerprintGenerator):
    """Test that header generator can work without any options."""
    assert fingerprint_generator.generate()



@pytest.mark.parametrize("fingerprint_generator",[
                pytest.param(bf_FingerprintGenerator, id="Browserforge"),
])
def test_fingerprint_generator_some_options(fingerprint_generator: AbstractFingerprintGenerator):
    """Test that header generator can work with only some options."""
    options = FingerprintGeneratorOptions(screen=ScreenOptions(min_width = 500), mockWebRTC=True)

    fingerprint = fingerprint_generator.generate(options=options)

    assert fingerprint.mockWebRTC == True
    assert fingerprint.screen.availWidth >= 500


@pytest.mark.parametrize("fingerprint_generator",[
                pytest.param(bf_FingerprintGenerator, id="Browserforge"),
])
def test_fingerprint_generator_all_options(fingerprint_generator: AbstractFingerprintGenerator):
    """Test that header generator can work with all the options. Some most basic checks of fingerprint.

    Fingerprint generation option might have no effect if there is no fingerprint sample present in collected data.
    """
    min_width = 600
    max_width = 1800
    min_height = 400
    max_height = 1200

    options = FingerprintGeneratorOptions(
        screen=ScreenOptions(
            min_width = min_width,
            max_width=max_width,
            min_height=min_height,
            max_height=max_height,
        ),
        mockWebRTC=True,
        slim=False,
        header_options = HeaderGeneratorOptions(
            strict = True,
            browsers = ["firefox"],
            operating_systems = ["windows"],
            devices = ["mobile"],
            locales = ["en"], #  This does not seem to generate any other values than `en-US` regardless of the input
            http_version = "2",  # Http1 does not work in browserforge

        )
    )

    fingerprint = fingerprint_generator.generate(options=options)

    assert fingerprint.screen.availWidth >= min_width
    assert fingerprint.screen.availWidth <= max_width
    assert fingerprint.screen.availHeight >= min_height
    assert fingerprint.screen.availHeight <= max_height

    assert fingerprint.mockWebRTC == True
    assert fingerprint.slim == False
    assert "Firefox" in fingerprint.navigator.userAgent
    assert "Win" in fingerprint.navigator.oscpu
    assert "en-US" in fingerprint.navigator.languages

