from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from crawlee._types import HttpHeaders
from crawlee._utils.docs import docs_group
from crawlee.fingerprint_suite._browserforge_adapter import BrowserforgeHeaderGenerator

if TYPE_CHECKING:
    from crawlee.fingerprint_suite._types import SupportedBrowserType


def fingerprint_browser_type_from_playwright_browser_type(
    playwright_browser_type: Literal['chromium', 'firefox', 'webkit'],
) -> SupportedBrowserType:
    if playwright_browser_type == 'chromium':
        return 'chrome'
    if playwright_browser_type == 'firefox':
        return 'firefox'
    if playwright_browser_type == 'webkit':
        return 'safari'
    raise ValueError(f'Unsupported browser type: {playwright_browser_type}')


@docs_group('Classes')
class HeaderGenerator:
    """Generate realistic looking or browser-like HTTP headers."""

    def __init__(self) -> None:
        self._generator = BrowserforgeHeaderGenerator()

    def _select_specific_headers(self, all_headers: dict[str, str], header_names: set[str]) -> HttpHeaders:
        return HttpHeaders({key: value for key, value in all_headers.items() if key in header_names})

    def get_specific_headers(
        self, header_names: set[str] | None = None, browser_type: SupportedBrowserType = 'chrome'
    ) -> HttpHeaders:
        """Return subset of headers based on the selected `header_names`.

        If no `header_names` are specified, full unfiltered headers are returned.
        """
        all_headers = self._generator.generate(browser_type=browser_type)

        if not header_names:
            return HttpHeaders(all_headers)
        return self._select_specific_headers(all_headers, header_names)

    def get_common_headers(self) -> HttpHeaders:
        """Get common HTTP headers ("Accept", "Accept-Language").

        We do not modify the "Accept-Encoding", "Connection" and other headers. They should be included and handled
        by the HTTP client or browser.
        """
        all_headers = self._generator.generate()
        return self._select_specific_headers(all_headers, header_names={'Accept', 'Accept-Language'})

    def get_random_user_agent_header(self) -> HttpHeaders:
        """Get a random User-Agent header."""
        all_headers = self._generator.generate()
        return self._select_specific_headers(all_headers, header_names={'User-Agent'})

    def get_user_agent_header(
        self,
        *,
        browser_type: SupportedBrowserType = 'chrome',
    ) -> HttpHeaders:
        """Get the User-Agent header based on the browser type."""
        if browser_type not in {'chrome', 'firefox', 'safari', 'edge'}:
            raise ValueError(f'Unsupported browser type: {browser_type}')
        all_headers = self._generator.generate(browser_type=browser_type)
        return self._select_specific_headers(all_headers, header_names={'User-Agent'})

    def get_sec_ch_ua_headers(
        self,
        *,
        browser_type: SupportedBrowserType = 'chrome',
    ) -> HttpHeaders:
        """Get the sec-ch-ua headers based on the browser type."""
        if browser_type not in {'chrome', 'firefox', 'safari', 'edge'}:
            raise ValueError(f'Unsupported browser type: {browser_type}')
        all_headers = self._generator.generate(browser_type=browser_type)
        return self._select_specific_headers(
            all_headers, header_names={'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform'}
        )
