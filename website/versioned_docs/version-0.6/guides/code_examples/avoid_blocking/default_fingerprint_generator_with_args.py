import asyncio

from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)


async def main() -> None:
    fingerprint_generator = DefaultFingerprintGenerator(
        header_options=HeaderGeneratorOptions(browsers=['chromium']),
        screen_options=ScreenOptions(min_width=400),
    )

    # ...


if __name__ == '__main__':
    asyncio.run(main())
