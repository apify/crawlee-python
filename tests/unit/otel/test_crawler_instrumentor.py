import sys
from unittest.mock import patch

import pytest


def test_import_error_handled() -> None:
    blocked = {
        mod_name: None
        for mod_name in sys.modules
        if mod_name == 'opentelemetry' or mod_name.startswith('opentelemetry.')
    }
    with patch.dict('sys.modules', blocked):
        sys.modules.pop('crawlee.otel.crawler_instrumentor', None)
        with pytest.raises(ImportError):
            from crawlee.otel.crawler_instrumentor import CrawlerInstrumentor  # noqa: F401 PLC0415
