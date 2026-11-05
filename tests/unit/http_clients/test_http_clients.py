from __future__ import annotations

import importlib
import sys
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from pydantic import ValidationError

if TYPE_CHECKING:
    from crawlee.http_clients import HttpClient


@pytest.mark.parametrize(
    ('optional_module_name', 'import_path'),
    [
        pytest.param('curl_cffi', 'crawlee.http_clients._curl_impersonate', id='curl_impersonate'),
        pytest.param('httpx2', 'crawlee.http_clients._httpx', id='httpx'),
    ],
)
def test_import_error_handled(optional_module_name: str, import_path: str) -> None:
    blocked = {
        mod_name: None
        for mod_name in sys.modules
        if mod_name == optional_module_name or mod_name.startswith(f'{optional_module_name}.')
    }
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith(import_path):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            importlib.import_module(import_path)


async def test_send_request_rejects_non_http_scheme(http_client: HttpClient) -> None:
    with pytest.raises(ValidationError):
        await http_client.send_request('gopher://127.0.0.1:6379/_PING')


async def test_stream_rejects_non_http_scheme(http_client: HttpClient) -> None:
    with pytest.raises(ValidationError):
        async with http_client.stream('gopher://127.0.0.1:6379/_PING'):
            pass
