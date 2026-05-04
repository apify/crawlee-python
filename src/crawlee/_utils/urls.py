from __future__ import annotations

import tempfile
from functools import lru_cache
from typing import TYPE_CHECKING

from pydantic import AnyHttpUrl, TypeAdapter
from tldextract import TLDExtract
from typing_extensions import assert_never
from yarl import URL

if TYPE_CHECKING:
    from collections.abc import Iterator
    from logging import Logger

    from crawlee._types import EnqueueStrategy


_ALLOWED_SCHEMES: frozenset[str] = frozenset({'http', 'https'})
"""URL schemes Crawlee accepts for fetching and enqueuing."""

UNSUPPORTED_SCHEME_MESSAGE = 'unsupported URL scheme (only http and https are allowed).'
"""Reusable suffix for log messages explaining why a non-`http(s)` URL was rejected."""

_HTTP_URL_ADAPTER: TypeAdapter[AnyHttpUrl] = TypeAdapter(AnyHttpUrl)
"""Pydantic validator for HTTP and HTTPS URLs."""


def is_url_absolute(url: str) -> bool:
    """Check if a URL is absolute."""
    url_parsed = URL(url)

    # We don't use .absolute because in yarl.URL, it is always True for links that start with '//'
    return bool(url_parsed.scheme) and bool(url_parsed.raw_authority)


def convert_to_absolute_url(base_url: str, relative_url: str) -> str:
    """Convert a relative URL to an absolute URL using a base URL."""
    return str(URL(base_url).join(URL(relative_url)))


def to_absolute_url_iterator(base_url: str, urls: Iterator[str], logger: Logger | None = None) -> Iterator[str]:
    """Convert an iterator of relative URLs to absolute URLs using a base URL."""
    for url in urls:
        if is_url_absolute(url):
            yield url
        else:
            converted_url = convert_to_absolute_url(base_url, url)
            # Skip the URL if conversion fails, probably due to an incorrect format, such as 'mailto:'.
            if not is_url_absolute(converted_url):
                if logger:
                    logger.debug(f'Could not convert URL "{url}" to absolute using base URL "{base_url}". Skipping it.')
                continue
            yield converted_url


def validate_http_url(value: str | None) -> str | None:
    """Validate the given HTTP URL.

    Args:
        value: The URL to validate, or `None` to skip validation.

    Raises:
        pydantic.ValidationError: If the URL is malformed or its scheme is not `http`/`https`.
    """
    if value is not None:
        _HTTP_URL_ADAPTER.validate_python(value)

    return value


def is_supported_url_scheme(url: str | URL) -> bool:
    """Return whether `url` uses a scheme Crawlee accepts (http or https)."""
    return _to_url(url).scheme in _ALLOWED_SCHEMES


def matches_enqueue_strategy(
    strategy: EnqueueStrategy,
    *,
    target_url: str | URL,
    origin_url: str | URL,
) -> bool:
    """Check whether `target_url` matches `origin_url` under the given enqueue strategy.

    This function checks only the strategy relationship between the two URLs. Callers must
    independently reject unsupported schemes via `is_supported_url_scheme` — `matches_enqueue_strategy`
    does not look at the scheme.

    Args:
        strategy: The enqueue strategy to apply.
        target_url: The URL to be evaluated.
        origin_url: The reference URL the target is compared against.

    Returns:
        `True` if `target_url` is allowed under `strategy` relative to `origin_url`, `False` otherwise.
    """
    target = _to_url(target_url)
    origin = _to_url(origin_url)

    if strategy == 'all':
        return True

    if origin.host is None or target.host is None:
        return False

    if strategy == 'same-hostname':
        return target.host == origin.host

    if strategy == 'same-domain':
        return _domain_under_public_suffix(origin.host) == _domain_under_public_suffix(target.host)

    if strategy == 'same-origin':
        return target.host == origin.host and target.scheme == origin.scheme and target.port == origin.port

    assert_never(strategy)


def _to_url(value: str | URL) -> URL:
    return URL(value) if isinstance(value, str) else value


@lru_cache(maxsize=1)
def _get_tld_extractor() -> TLDExtract:
    """Return a lazily-initialized `TLDExtract` instance shared across the module."""
    # `mkdtemp` (vs `TemporaryDirectory`) returns a path whose lifetime is tied to the process — `TemporaryDirectory`
    # is collected immediately when its return value is discarded, which would race the directory out from under
    # tldextract.
    return TLDExtract(cache_dir=tempfile.mkdtemp())


@lru_cache(maxsize=2048)
def _domain_under_public_suffix(host: str) -> str:
    """Return the registrable domain for `host`, cached to avoid re-running the PSL lookup."""
    return _get_tld_extractor().extract_str(host).top_domain_under_public_suffix
