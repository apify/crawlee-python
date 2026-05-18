from __future__ import annotations

import sys
from unittest.mock import patch

import pytest


def test_import_error_handled() -> None:
    blocked = {mod_name: None for mod_name in sys.modules if mod_name == 'redis' or mod_name.startswith('redis.')}
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith('crawlee.storage_clients._redis'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.storage_clients._redis import RedisStorageClient  # noqa: F401 PLC0415
