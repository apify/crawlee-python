from __future__ import annotations

# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/utils/src/internals/blocked.ts

CLOUDFLARE_RETRY_CSS_SELECTORS = [
    '#turnstile-wrapper iframe[src^="https://challenges.cloudflare.com"]',
]

RETRY_CSS_SELECTORS = [
    *CLOUDFLARE_RETRY_CSS_SELECTORS,
    'div#infoDiv0 a[href*="//www.google.com/policies/terms/"]',
    'iframe[src*="_Incapsula_Resource"]',
]
"""
CSS selectors for elements that should trigger a retry, as the crawler is likely getting blocked.
"""

ROTATE_PROXY_ERRORS = [
    'ECONNRESET',
    'ECONNREFUSED',
    'ERR_PROXY_CONNECTION_FAILED',
    'ERR_TUNNEL_CONNECTION_FAILED',
    'Proxy responded with',
]
"""
Content of proxy errors that should trigger a retry, as the proxy is likely getting blocked / is malfunctioning.
"""
