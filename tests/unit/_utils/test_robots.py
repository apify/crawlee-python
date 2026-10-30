from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee._utils.robots import RobotsTxtFile
from crawlee.http_clients import HttpClient, HttpResponse


async def test_extract_same_host_sitemaps_urls() -> None:
    """Sitemap entries on the same host as the robots.txt are returned."""
    content = 'User-agent: *\nSitemap: http://example.com/sitemap_1.xml\nSitemap: http://example.com/sitemap_2.xml\n'
    robots = await RobotsTxtFile.from_content('http://example.com/robots.txt', content)
    assert set(robots.get_sitemaps(enqueue_strategy='same-hostname')) == {
        'http://example.com/sitemap_1.xml',
        'http://example.com/sitemap_2.xml',
    }


async def test_extract_sitemaps_urls_filters_cross_host_and_non_http() -> None:
    """Cross-host and non-http(s) `Sitemap:` directives in robots.txt are silently filtered."""
    content = (
        'User-agent: *\n'
        'Sitemap: http://example.com/legit.xml\n'
        'Sitemap: http://other.test/payload.xml\n'
        'Sitemap: gopher://internal:6379/_PING\n'
        'Sitemap: ftp://example.com/payload.xml\n'
    )
    robots = await RobotsTxtFile.from_content('http://example.com/robots.txt', content)
    assert robots.get_sitemaps(enqueue_strategy='same-hostname') == ['http://example.com/legit.xml']


async def test_get_sitemaps_with_strategy_all_returns_cross_host() -> None:
    """`enqueue_strategy='all'` disables host filtering but still rejects non-http(s) schemes."""
    content = (
        'User-agent: *\n'
        'Sitemap: http://example.com/legit.xml\n'
        'Sitemap: http://other.test/payload.xml\n'
        'Sitemap: gopher://internal:6379/_PING\n'
        'Sitemap: ftp://example.com/payload.xml\n'
    )
    robots = await RobotsTxtFile.from_content('http://example.com/robots.txt', content)
    assert set(robots.get_sitemaps(enqueue_strategy='all')) == {
        'http://example.com/legit.xml',
        'http://other.test/payload.xml',
    }


async def test_parse_from_content() -> None:
    content = """User-agent: *
        Disallow: *deny_all/
        crawl-delay: 10
        User-agent: Googlebot
        Disallow: *deny_googlebot/"""
    robots = await RobotsTxtFile.from_content('http://not-exists.com/robots.txt', content)
    assert robots.is_allowed('http://not-exists.com/something/page.html')
    assert robots.is_allowed('http://not-exists.com/deny_googlebot/page.html')
    assert not robots.is_allowed('http://not-exists.com/deny_googlebot/page.html', 'Googlebot')
    assert not robots.is_allowed('http://not-exists.com/deny_all/page.html')


async def test_get_fractional_crawl_delay() -> None:
    """A fractional `Crawl-delay` value is returned without truncation."""
    content = 'User-agent: *\nCrawl-delay: 0.5\nDisallow: /private/'
    robots = await RobotsTxtFile.from_content('http://example.com/robots.txt', content)
    assert robots.get_crawl_delay() == 0.5


async def test_bind_robots_txt_url() -> None:
    content = 'User-agent: *\nDisallow: /'
    robots = await RobotsTxtFile.from_content('http://check.com/robots.txt', content)
    assert not robots.is_allowed('http://check.com/test.html')
    assert robots.is_allowed('http://othercheck.com/robots.txt')


@pytest.mark.parametrize(
    'status_code',
    [
        pytest.param(200, id='success'),
        pytest.param(404, id='client-error'),
        pytest.param(500, id='server-error'),
    ],
)
async def test_load_closes_response(status_code: int) -> None:
    """Test that `RobotsTxtFile.load` closes the response, also when the status code leaves the body unread."""
    response = MagicMock(spec=HttpResponse)
    response.status_code = status_code
    response.read = AsyncMock(return_value=b'User-agent: *\nAllow: /')
    http_client = AsyncMock(spec=HttpClient)
    http_client.send_request.return_value = response

    await RobotsTxtFile.load('http://example.com/robots.txt', http_client)

    response.__aexit__.assert_awaited_once()
