from __future__ import annotations

from typing import TYPE_CHECKING, Any

from playwright.async_api import Page

if TYPE_CHECKING:
    from browserforge.fingerprints import Fingerprint
    from playwright.async_api import Page


def browserforge_patch_options(fingerprint: Fingerprint, options: dict[str, Any]) -> dict[str, Any]:
    """Builds options for new context."""
    return {
        'user_agent': fingerprint.navigator.userAgent,
        'color_scheme': 'dark',
        'viewport': {
            'width': fingerprint.screen.width,
            'height': fingerprint.screen.height,
            **options.pop('viewport', {}),
        },
        'extra_http_headers': {
            'accept-language': fingerprint.headers['Accept-Language'],
            **options.pop('extra_http_headers', {}),
        },
        'device_scale_factor': fingerprint.screen.devicePixelRatio,
        **options,
    }


async def browserforge_dark_mode(page: Page) -> None:
    await page.emulate_media(color_scheme='dark')
