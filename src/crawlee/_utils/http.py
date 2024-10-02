from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee._types import HttpHeaders


def normalize_headers(headers: HttpHeaders) -> HttpHeaders:
    """Converts all header keys to capital case and returns them with the sorted."""
    normalized_headers = {k.capitalize(): v for k, v in headers.items()}
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
