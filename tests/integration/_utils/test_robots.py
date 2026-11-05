from __future__ import annotations

import base64
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


async def test_server_error_disallows_all(server_url: URL, http_client: HttpClient) -> None:
    """A 5xx robots.txt response is treated as complete disallow (RFC 9309, section 2.3.1.4).

    The response body must not be parsed as rules; otherwise an error page would be misread as allow-all.
    """
    robots = await RobotsTxtFile.load(str(server_url / 'status/500'), http_client)
    assert not robots.is_allowed(str(server_url / 'admin/page.html'))
    assert not robots.is_allowed(str(server_url / 'something/page.html'))


async def test_load_robots_txt_with_utf8_bom(server_url: URL, http_client: HttpClient) -> None:
    """A UTF-8 byte order mark at the start of robots.txt does not hide the first group's rules."""
    content = b'\xef\xbb\xbfUser-agent: *\nDisallow: /private/\n'
    robots_url = (server_url / 'echo_content').with_query(base64=base64.b64encode(content).decode())

    robots = await RobotsTxtFile.load(str(robots_url), http_client)

    assert not robots.is_allowed(str(server_url / 'private/page.html'))
    assert robots.is_allowed(str(server_url / 'public/page.html'))


async def test_extract_sitemaps_urls(server_url: URL, http_client: HttpClient) -> None:
    """Cross-host sitemap entries are dropped under the `'same-hostname'` enqueue strategy."""
    robots = await RobotsTxtFile.find(str(server_url), http_client)
    # The fixture lists `http://not-exists.com/sitemap_*.xml`, which is cross-host relative to `server_url`.
    assert robots.get_sitemaps(enqueue_strategy='same-hostname') == []
