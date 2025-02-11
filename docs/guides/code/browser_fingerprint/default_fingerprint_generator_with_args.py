from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)

fingerprint_generator = DefaultFingerprintGenerator(
    header_options=HeaderGeneratorOptions(browsers=['chromium']),
    screen_options=ScreenOptions(min_width=400),
)
