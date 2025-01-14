from crawlee.fingerprint_suite._browserforge_adapter import FingerprintGenerator
from crawlee.fingerprint_suite._types import FingerprintGeneratorOptions, HeaderGeneratorOptions, ScreenOptions


def test_fingerprint_generator_has_default() -> None:
    """Test that header generator can work without any options."""
    assert FingerprintGenerator().generate()


def test_fingerprint_generator_some_options() -> None:
    """Test that header generator can work with only some options."""
    options = FingerprintGeneratorOptions(screen=ScreenOptions(min_width=500), mock_web_rtc=True)

    fingerprint = FingerprintGenerator(options=options).generate()

    assert fingerprint.mockWebRTC is True
    assert fingerprint.screen.availWidth >= 500


def test_fingerprint_generator_all_options() -> None:
    """Test that header generator can work with all the options. Some most basic checks of fingerprint.

    Fingerprint generation option might have no effect if there is no fingerprint sample present in collected data.
    """
    min_width = 600
    max_width = 1800
    min_height = 400
    max_height = 1200

    options = FingerprintGeneratorOptions(
        screen=ScreenOptions(
            min_width=min_width,
            max_width=max_width,
            min_height=min_height,
            max_height=max_height,
        ),
        mock_web_rtc=True,
        slim=False,
        header_options=HeaderGeneratorOptions(
            strict=True,
            browsers=['firefox'],
            operating_systems=['windows'],
            devices=['mobile'],
            locales=['en'],  #  This does not seem to generate any other values than `en-US` regardless of the input
            http_version='2',  # Http1 does not work in browserforge
        ),
    )

    fingerprint = FingerprintGenerator(options=options).generate()

    assert fingerprint.screen.availWidth >= min_width
    assert fingerprint.screen.availWidth <= max_width
    assert fingerprint.screen.availHeight >= min_height
    assert fingerprint.screen.availHeight <= max_height

    assert fingerprint.mockWebRTC is True
    assert fingerprint.slim is False
    assert 'Firefox' in fingerprint.navigator.userAgent
    assert 'Win' in fingerprint.navigator.oscpu
    assert 'en-US' in fingerprint.navigator.languages
