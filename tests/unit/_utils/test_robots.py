from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._utils.robots import RobotsTxtFile

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.http_clients._base import HttpClient


async def test_generation_robots_txt_url(server_url: URL, http_client: HttpClient) -> None:
    """`RobotsTxtFile.find` constructs the correct /robots.txt URL and successfully parses the response."""
    robots_file = await RobotsTxtFile.find(str(server_url), http_client)
    # The fixture's robots.txt disallows /deny_all/ — proves the file was fetched and parsed.
    assert not robots_file.is_allowed(str(server_url / 'deny_all/page.html'))


async def test_allow_disallow_robots_txt(server_url: URL, http_client: HttpClient) -> None:
    robots = await RobotsTxtFile.find(str(server_url), http_client)
    assert robots.is_allowed('https://crawlee.dev')
    assert robots.is_allowed(str(server_url / 'something/page.html'))
    assert robots.is_allowed(str(server_url / 'deny_googlebot/page.html'))
    assert not robots.is_allowed(str(server_url / 'deny_all/page.html'))


async def test_extract_sitemaps_urls(server_url: URL, http_client: HttpClient) -> None:
    """Cross-host sitemap entries are dropped from the test fixture's robots.txt."""
    robots = await RobotsTxtFile.find(str(server_url), http_client)
    # The fixture lists `http://not-exists.com/sitemap_*.xml`, which is cross-host relative to `server_url` and
    # therefore filtered out per the robots.txt specification.
    assert robots.get_sitemaps() == []


async def test_extract_same_host_sitemaps_urls() -> None:
    """Sitemap entries on the same host as the robots.txt are returned."""
    content = 'User-agent: *\nSitemap: http://example.com/sitemap_1.xml\nSitemap: http://example.com/sitemap_2.xml\n'
    robots = await RobotsTxtFile.from_content('http://example.com/robots.txt', content)
    assert set(robots.get_sitemaps()) == {
        'http://example.com/sitemap_1.xml',
        'http://example.com/sitemap_2.xml',
    }


async def test_extract_sitemaps_urls_filters_cross_host() -> None:
    """Cross-host `Sitemap:` directives in robots.txt are silently filtered."""
    content = (
        'User-agent: *\n'
        'Sitemap: http://example.com/legit.xml\n'
        'Sitemap: http://other.test/payload.xml\n'
        'Sitemap: gopher://internal:6379/_PING\n'
    )
    robots = await RobotsTxtFile.from_content('http://example.com/robots.txt', content)
    assert robots.get_sitemaps() == ['http://example.com/legit.xml']


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


async def test_bind_robots_txt_url() -> None:
    content = 'User-agent: *\nDisallow: /'
    robots = await RobotsTxtFile.from_content('http://check.com/robots.txt', content)
    assert not robots.is_allowed('http://check.com/test.html')
    assert robots.is_allowed('http://othercheck.com/robots.txt')
