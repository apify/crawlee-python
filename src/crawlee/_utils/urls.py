from __future__ import annotations

from urllib.parse import parse_qs, urljoin, urlparse

from pydantic import AnyHttpUrl, TypeAdapter


def is_url_absolute(url: str) -> bool:
    """Check if a URL is absolute."""
    return bool(urlparse(url).netloc)


def convert_to_absolute_url(base_url: str, relative_url: str) -> str:
    """Convert a relative URL to an absolute URL using a base URL."""
    return urljoin(base_url, relative_url)


def extract_query_params(url: str) -> dict[str, list[str]]:
    """Extract query parameters from a given URL."""
    url_parsed = urlparse(url)
    return parse_qs(url_parsed.query)


_http_url_adapter = TypeAdapter(AnyHttpUrl)


def validate_http_url(value: str | None) -> str | None:
    """Validate the given HTTP URL.

    Raises:
        pydantic.error_wrappers.ValidationError: If the URL is not valid.
    """
    if value is not None:
        _http_url_adapter.validate_python(value)

    return value
