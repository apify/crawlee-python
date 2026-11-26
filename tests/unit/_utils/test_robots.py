from __future__ import annotations

import pytest

from crawlee._utils.robots import RobotsTxtFile


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


@pytest.mark.parametrize(
    ('robots_url', 'url'),
    [
        pytest.param(
            'https://example.com/robots.txt', 'https://example.com:443/private/page.html', id='https_port_in_url'
        ),
        pytest.param(
            'https://example.com:443/robots.txt', 'https://example.com/private/page.html', id='https_port_in_robots'
        ),
        pytest.param('http://example.com/robots.txt', 'http://example.com:80/private/page.html', id='http_port_in_url'),
    ],
)
async def test_default_port_matches_robots_txt_origin(robots_url: str, url: str) -> None:
    """A URL that spells out the default port follows the robots.txt rules of its origin."""
    robots = await RobotsTxtFile.from_content(robots_url, 'User-agent: *\nDisallow: /private/')
    assert not robots.is_allowed(url)


async def test_non_default_port_is_another_origin() -> None:
    """A URL on a non-default port is not covered by the robots.txt of the default port."""
    robots = await RobotsTxtFile.from_content('https://example.com/robots.txt', 'User-agent: *\nDisallow: /private/')
    assert robots.is_allowed('https://example.com:8443/private/page.html')


async def test_bind_robots_txt_url() -> None:
    content = 'User-agent: *\nDisallow: /'
    robots = await RobotsTxtFile.from_content('http://check.com/robots.txt', content)
    assert not robots.is_allowed('http://check.com/test.html')
    assert robots.is_allowed('http://othercheck.com/robots.txt')
