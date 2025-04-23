from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._utils.robots import RobotsTxtFile

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.http_clients._base import HttpClient


async def test_generation_robots_txt_url(server_url: URL, http_client: HttpClient) -> None:
    robots_file = await RobotsTxtFile.find(str(server_url), http_client)
    assert len(robots_file.get_sitemaps()) > 0


async def test_allow_disallow_robots_txt(server_url: URL, http_client: HttpClient) -> None:
    robots = await RobotsTxtFile.find(str(server_url), http_client)
    assert robots.is_allowed('https://crawlee.dev')
    assert robots.is_allowed(str(server_url / 'something/page.html'))
    assert robots.is_allowed(str(server_url / 'deny_googlebot/page.html'))
    assert not robots.is_allowed(str(server_url / 'deny_all/page.html'))


async def test_extract_sitemaps_urls(server_url: URL, http_client: HttpClient) -> None:
    robots = await RobotsTxtFile.find(str(server_url), http_client)
    assert len(robots.get_sitemaps()) == 2
    assert set(robots.get_sitemaps()) == {'http://not-exists.com/sitemap_1.xml', 'http://not-exists.com/sitemap_2.xml'}


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
