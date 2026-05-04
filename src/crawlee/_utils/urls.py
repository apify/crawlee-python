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


def matches_enqueue_strategy(
    strategy: EnqueueStrategy,
    *,
    target_url: str | URL,
    origin_url: str | URL,
) -> bool:
    """Check whether `target_url` matches `origin_url` under the given enqueue strategy.

    Targets with non-http(s) schemes are always rejected, including under `strategy='all'`.

    Args:
        strategy: The enqueue strategy to apply.
        target_url: The URL to be evaluated.
        origin_url: The reference URL the target is compared against.

    Returns:
        `True` if `target_url` is allowed under `strategy` relative to `origin_url`, `False` otherwise.
    """
    target = URL(target_url) if isinstance(target_url, str) else target_url
    origin = URL(origin_url) if isinstance(origin_url, str) else origin_url

    if target.scheme not in _ALLOWED_SCHEMES:
        return False

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
