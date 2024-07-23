from __future__ import annotations

from urllib.parse import urljoin, urlparse

from pydantic import HttpUrl


def is_url_absolute(url: str | HttpUrl) -> bool:
    """Check if a URL is absolute."""
    url = url if isinstance(url, str) else str(url)
    return bool(urlparse(url).netloc)


def make_url_absolute(base_url: str | HttpUrl, relative_url: str | HttpUrl) -> HttpUrl:
    """Make a relative URL absolute by combining it with a base URL."""
    base_url = base_url if isinstance(base_url, str) else str(base_url)
    relative_url = relative_url if isinstance(relative_url, str) else str(relative_url)
    absolute_url = urljoin(base_url, relative_url)
    return HttpUrl(absolute_url)
