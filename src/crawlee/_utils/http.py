from __future__ import annotations

from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from collections.abc import Mapping

    from crawlee._types import HttpHeaders


@overload
def normalize_headers(headers: Mapping[str, str]) -> HttpHeaders:
    pass


@overload
def normalize_headers(headers: None) -> None:
    pass


def normalize_headers(headers: Mapping[str, str] | None) -> HttpHeaders | None:
    """Converts all header keys to lowercase and returns them sorted by key."""
    if headers is None:
        return None
    normalized_headers = {k.lower(): v for k, v in headers.items()}
    sorted_headers = sorted(normalized_headers.items())
    return dict(sorted_headers)


def is_status_code_error(value: int) -> bool:
    """Returns `True` for 4xx or 5xx status codes, `False` otherwise."""
    return is_status_code_client_error(value) or is_status_code_server_error(value)


def is_status_code_client_error(value: int) -> bool:
    """Returns `True` for 4xx status codes, `False` otherwise."""
    return 400 <= value <= 499  # noqa: PLR2004


def is_status_code_server_error(value: int) -> bool:
    """Returns `True` for 5xx status codes, `False` otherwise."""
    return value >= 500  # noqa: PLR2004
