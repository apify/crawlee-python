from __future__ import annotations

from http import HTTPStatus


def is_status_code_client_error(value: int) -> bool:
    """Return `True` for 4xx status codes, `False` otherwise."""
    return HTTPStatus.BAD_REQUEST <= value < HTTPStatus.INTERNAL_SERVER_ERROR


def is_status_code_server_error(value: int) -> bool:
    """Return `True` for 5xx status codes, `False` otherwise."""
    return value >= HTTPStatus.INTERNAL_SERVER_ERROR


def is_status_code_successful(value: int) -> bool:
    """Return `True` for 2xx and 3xx status codes, `False` otherwise."""
    return HTTPStatus.OK <= value < HTTPStatus.BAD_REQUEST
