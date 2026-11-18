from __future__ import annotations

import sys
from datetime import timedelta
from unittest import mock

import pytest

from crawlee import (
    service_locator,
)
from crawlee.configuration import Configuration
from crawlee.crawlers import (
    PlaywrightCrawler,
)
from crawlee.http_clients import ImpitHttpClient
from crawlee.proxy_configuration import ProxyConfiguration


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


async def test_robots_txt_fetched_through_proxy() -> None:
    """The robots.txt file is fetched through the configured proxy, with a bounded timeout."""
    proxy_url = 'http://proxy.placeholder.com:8000'
    crawler = PlaywrightCrawler(
        proxy_configuration=ProxyConfiguration(proxy_urls=[proxy_url]),
        respect_robots_txt_file=True,
    )
    send_request = mock.AsyncMock(side_effect=RuntimeError('No network in this test.'))

    with mock.patch.object(ImpitHttpClient, 'send_request', send_request):
        await crawler._get_robots_txt_file_for_url('https://site.placeholder.com/page')

    send_request.assert_awaited_once()
    assert send_request.call_args.args == ('https://site.placeholder.com/robots.txt',)
    assert send_request.call_args.kwargs['proxy_info'].url == proxy_url
    assert send_request.call_args.kwargs['timeout'] == timedelta(seconds=30)
