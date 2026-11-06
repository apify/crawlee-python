from __future__ import annotations

import logging
from typing import Any

import httpx2
import pytest

from crawlee.http_clients import HttpxHttpClient


@pytest.mark.parametrize(
    ('client_kwargs', 'expected_warning'),
    [
        pytest.param({'mounts': {'all://': httpx2.AsyncHTTPTransport()}}, '`mounts` argument', id='mounts'),
        pytest.param({'transport': httpx2.AsyncHTTPTransport()}, '`transport` argument', id='transport'),
    ],
)
async def test_transport_kwargs_do_not_reach_the_client(client_kwargs: dict[str, Any], expected_warning: str) -> None:
    """Test that kwargs mounting a transport of their own are rejected with a warning, so the proxy keeps working."""
    with pytest.warns(UserWarning, match=expected_warning):
        client = HttpxHttpClient(**client_kwargs)

    async with client:
        assert client._get_client(None)._mounts == {}
        assert client._get_client(None)._transport is not client_kwargs.get('transport')


def test_silences_httpx_request_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instantiating the client lowers the noisy per-request `httpx2` INFO logs to WARNING by default."""
    monkeypatch.setattr(HttpxHttpClient, '_httpx2_log_level', None)
    httpx_logger = logging.getLogger('httpx2')
    original_level = httpx_logger.level
    httpx_logger.setLevel(logging.NOTSET)

    try:
        HttpxHttpClient()
        assert httpx_logger.level == logging.WARNING
    finally:
        httpx_logger.setLevel(original_level)


def test_keeps_user_set_httpx_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instantiating the client leaves the `httpx2` logger level alone when the application has set one."""
    monkeypatch.setattr(HttpxHttpClient, '_httpx2_log_level', None)
    httpx_logger = logging.getLogger('httpx2')
    original_level = httpx_logger.level
    httpx_logger.setLevel(logging.INFO)

    try:
        HttpxHttpClient()
        assert httpx_logger.level == logging.INFO
    finally:
        httpx_logger.setLevel(original_level)


def test_follows_configured_log_level_of_later_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """A client created after the configured log level changes updates the `httpx2` level an earlier client set."""
    monkeypatch.setattr(HttpxHttpClient, '_httpx2_log_level', None)
    httpx_logger = logging.getLogger('httpx2')
    original_level = httpx_logger.level
    httpx_logger.setLevel(logging.NOTSET)

    try:
        monkeypatch.setattr('crawlee.http_clients._httpx.get_configured_log_level', lambda: logging.INFO)
        HttpxHttpClient()
        assert httpx_logger.level == logging.WARNING

        monkeypatch.setattr('crawlee.http_clients._httpx.get_configured_log_level', lambda: logging.DEBUG)
        HttpxHttpClient()
        assert httpx_logger.level == logging.DEBUG
    finally:
        httpx_logger.setLevel(original_level)
