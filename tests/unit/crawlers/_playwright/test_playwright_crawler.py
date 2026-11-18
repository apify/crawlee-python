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


async def test_robots_txt_fetched_through_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """The robots.txt file is fetched through the configured proxy."""
    proxy_url = 'http://proxy.placeholder.com:8000'
    response = mock.Mock(status_code=200, read=mock.AsyncMock(return_value=b'User-agent: *\nDisallow: /private'))
    send_request = mock.AsyncMock(return_value=response)
    monkeypatch.setattr(ImpitHttpClient, 'send_request', send_request)
    crawler = PlaywrightCrawler(
        proxy_configuration=ProxyConfiguration(proxy_urls=[proxy_url]),
        respect_robots_txt_file=True,
    )

    assert await crawler._is_allowed_based_on_robots_txt_file('https://site.placeholder.com/private') is False

    send_request.assert_awaited_once()
    assert send_request.call_args.args == ('https://site.placeholder.com/robots.txt',)
    assert send_request.call_args.kwargs['proxy_info'].url == proxy_url


def test_robots_txt_http_client_is_cleaned_up_with_crawler() -> None:
    """The HTTP client that replaces `PlaywrightHttpClient` for robots.txt is managed by the crawler."""
    crawler = PlaywrightCrawler(respect_robots_txt_file=True)

    assert isinstance(crawler._robots_txt_http_client, ImpitHttpClient)
    assert crawler._robots_txt_http_client in crawler._additional_context_managers


def test_robots_txt_uses_custom_http_client() -> None:
    """A custom HTTP client of the crawler also fetches robots.txt."""
    http_client = ImpitHttpClient()
    crawler = PlaywrightCrawler(http_client=http_client, respect_robots_txt_file=True)

    assert crawler._robots_txt_http_client is http_client
    assert http_client not in crawler._additional_context_managers
