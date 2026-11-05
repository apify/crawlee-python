from __future__ import annotations

import sys
from unittest import mock

import pytest

from crawlee import (
    service_locator,
)
from crawlee.configuration import Configuration
from crawlee.crawlers import (
    PlaywrightCrawler,
)


async def test_passing_configuration() -> None:
    """Check that the configuration is allowed to be passed to the Playwrightcrawler."""
    service_locator.set_configuration(Configuration(log_level='INFO'))
    configuration = Configuration(log_level='WARNING')

    crawler = PlaywrightCrawler(configuration=configuration)

    assert service_locator.get_configuration().log_level == 'INFO'
    assert crawler._service_locator.get_configuration().log_level == 'WARNING'


def test_import_error_handled() -> None:
    blocked = {
        mod_name: None for mod_name in sys.modules if mod_name == 'playwright' or mod_name.startswith('playwright.')
    }
    with mock.patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith('crawlee.crawlers._playwright'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers._playwright import PlaywrightCrawler  # noqa: F401 PLC0415
