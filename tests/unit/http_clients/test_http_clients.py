from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

import pytest


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
