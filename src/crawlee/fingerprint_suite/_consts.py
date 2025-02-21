from __future__ import annotations

# ruff: noqa: E501

COMMON_ACCEPT = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'

COMMON_ACCEPT_LANGUAGE = 'en-US,en;q=0.9'

BROWSER_TYPE_HEADER_KEYWORD = {
    'chromium': {'Chrome', 'CriOS'},
    'firefox': {'Firefox', 'FxiOS'},
    'edge': {'Edg', 'Edge', 'EdgA', 'EdgiOS'},
    'webkit': {'Safari'},
}
