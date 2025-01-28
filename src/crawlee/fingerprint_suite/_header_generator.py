from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._types import HttpHeaders
from crawlee._utils.docs import docs_group
from crawlee.fingerprint_suite._browserforge_adapter import BrowserforgeHeaderGenerator
from crawlee.fingerprint_suite._consts import (
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM,
)

if TYPE_CHECKING:
    from crawlee.fingerprint_suite._types import SupportedBrowserType


@docs_group('Classes')
class HeaderGenerator:
    """Generates realistic looking or browser-like HTTP headers."""

    def __init__(self):
        self._generator = BrowserforgeHeaderGenerator()

    def get_common_headers(self) -> HttpHeaders:
        """Get common HTTP headers ("Accept", "Accept-Language").

        We do not modify the "Accept-Encoding", "Connection" and other headers. They should be included and handled
        by the HTTP client or browser.
        """
        all_headers = self._generator.generate()
        return HttpHeaders({key:value for key, value in all_headers.items() if key in {'Accept', 'Accept-Language'}})

    def get_random_user_agent_header(self) -> HttpHeaders:
        """Get a random User-Agent header."""
        all_headers = self._generator.generate()
        return HttpHeaders({'User-Agent':all_headers['User-Agent']})


    def get_user_agent_header(
        self,
        *,
        browser_type: SupportedBrowserType = 'chromium',
    ) -> HttpHeaders:
        """Get the User-Agent header based on the browser type."""
        headers = dict[str, str]()

        if browser_type not in {'chromium', 'firefox', 'webkit', 'edge'}:
            raise ValueError(f'Unsupported browser type: {browser_type}')
        all_headers = self._generator.generate(browser_type=browser_type)
        return HttpHeaders({'User-Agent': all_headers['User-Agent']})

    def get_sec_ch_ua_headers(
        self,
        *,
        browser_type: SupportedBrowserType = 'chromium',
    ) -> HttpHeaders:
        """Get the Sec-Ch-Ua headers based on the browser type."""
        headers = dict[str, str]()

        if browser_type == 'chromium':
            # Currently, only Chromium uses Sec-Ch-Ua headers.
            headers['Sec-Ch-Ua'] = PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA
            headers['Sec-Ch-Ua-Mobile'] = PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE
            headers['Sec-Ch-Ua-Platform'] = PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM

        elif browser_type == 'firefox':  # noqa: SIM114
            pass

        elif browser_type == 'webkit':
            pass

        else:
            raise ValueError(f'Unsupported browser type: {browser_type}')

        return HttpHeaders(headers)
