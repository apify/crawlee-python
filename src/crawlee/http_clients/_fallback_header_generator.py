import random
from typing import Literal

from crawlee import HttpHeaders
from crawlee.http_clients._fallback_consts import (
    COMMON_ACCEPT,
    COMMON_ACCEPT_LANGUAGE,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM,
    PW_CHROMIUM_HEADLESS_DEFAULT_USER_AGENT,
    PW_FIREFOX_HEADLESS_DEFAULT_USER_AGENT,
    PW_WEBKIT_HEADLESS_DEFAULT_USER_AGENT,
    USER_AGENT_POOL,
)

BrowserType = Literal['chromium', 'firefox', 'webkit']


class HeaderGenerator:
    """Generates realistic looking or browser-like HTTP headers."""

    def get_common_headers(self) -> HttpHeaders:
        """Get common HTTP headers ("Accept", "Accept-Language").

        We do not modify the "Accept-Encoding", "Connection" and other headers. They should be included and handled
        by the HTTP client or browser.
        """
        headers = {
            'Accept': COMMON_ACCEPT,
            'Accept-Language': COMMON_ACCEPT_LANGUAGE,
        }
        return HttpHeaders(headers)

    def get_random_user_agent_header(self) -> HttpHeaders:
        """Get a random User-Agent header."""
        headers = {'User-Agent': random.choice(USER_AGENT_POOL)}
        return HttpHeaders(headers)

    def get_user_agent_header(
        self,
        *,
        browser_type: BrowserType = 'chromium',
    ) -> HttpHeaders:
        """Get the User-Agent header based on the browser type."""
        headers = dict[str, str]()

        if browser_type == 'chromium':
            headers['User-Agent'] = PW_CHROMIUM_HEADLESS_DEFAULT_USER_AGENT

        elif browser_type == 'firefox':
            headers['User-Agent'] = PW_FIREFOX_HEADLESS_DEFAULT_USER_AGENT

        elif browser_type == 'webkit':
            headers['User-Agent'] = PW_WEBKIT_HEADLESS_DEFAULT_USER_AGENT

        else:
            raise ValueError(f'Unsupported browser type: {browser_type}')

        return HttpHeaders(headers)

    def get_sec_ch_ua_headers(
        self,
        *,
        browser_type: BrowserType = 'chromium',
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
