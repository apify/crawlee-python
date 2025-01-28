from __future__ import annotations

# ruff: noqa: E501

COMMON_ACCEPT = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'

COMMON_ACCEPT_LANGUAGE = 'en-US,en;q=0.9'

# Playwright default headers (user-agents and sec-ch) for headless browsers.
PW_CHROMIUM_HEADLESS_DEFAULT_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA = '"Not=A?Brand";v="8", "Chromium";v="124", "Google Chrome";v="124"'
PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE = '?0'
PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM = '"macOS"'

PW_FIREFOX_HEADLESS_DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv125.0) Gecko/20100101 Firefox/125.0'
)
PW_WEBKIT_HEADLESS_DEFAULT_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'


BROWSER_TYPE_HEADER_KEYWORD = {
    'chromium':{'Chrome', 'CriOS'},
    'firefox':{'Firefox', 'FxiOS'},
    'edge':{'Edg', 'Edge', 'EdgA', 'EdgiOS'},
    'webkit':{'Safari'},
}
