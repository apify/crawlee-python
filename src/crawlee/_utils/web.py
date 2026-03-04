from __future__ import annotations


def is_status_code_client_error(value: int) -> bool:
    """Return `True` for 4xx status codes, `False` otherwise."""
    return 400 <= value <= 499  # noqa: PLR2004


def is_status_code_server_error(value: int) -> bool:
    """Return `True` for 5xx status codes, `False` otherwise."""
    return value >= 500  # noqa: PLR2004


def is_status_code_successful(value: int) -> bool:
    """Return `True` for 2xx and 3xx status codes, `False` otherwise."""
    return 200 <= value < 400  # noqa: PLR2004
