import base64
import gzip

from yarl import URL

from crawlee.http_clients._base import HttpClient
from crawlee.request_loaders._sitemap_request_loader import SitemapRequestLoader

BASIC_SITEMAP = """
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url>
<loc>http://not-exists.com/</loc>
<lastmod>2005-02-03</lastmod>
<changefreq>monthly</changefreq>
<priority>0.8</priority>
</url>
<url>
<loc>http://not-exists.com/catalog?item=12&amp;desc=vacation_hawaii</loc>
<changefreq>weekly</changefreq>
</url>
<url>
<loc>http://not-exists.com/catalog?item=73&amp;desc=vacation_new_zealand</loc>
<lastmod>2004-12-23</lastmod>
<changefreq>weekly</changefreq>
</url>
<url>
<loc>http://not-exists.com/catalog?item=74&amp;desc=vacation_newfoundland</loc>
<lastmod>2004-12-23T18:00:15+00:00</lastmod>
<priority>0.3</priority>
</url>
<url>
<loc>http://not-exists.com/catalog?item=83&amp;desc=vacation_usa</loc>
<lastmod>2004-11-23</lastmod>
</url>
</urlset>
""".strip()


def compress_gzip(data: str) -> bytes:
    """Compress a string using gzip."""
    return gzip.compress(data.encode())


def encode_base64(data: bytes) -> str:
    """Encode bytes to a base64 string."""
    return base64.b64encode(data).decode('utf-8')


async def test_sitemap_traversal(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client)

    while not await sitemap_loader.is_finished():
        item = await sitemap_loader.fetch_next_request()
        assert item is not None

        await sitemap_loader.mark_request_as_handled(item)

    assert await sitemap_loader.is_empty()
    assert await sitemap_loader.is_finished()
    assert await sitemap_loader.get_total_count() == 5
    assert await sitemap_loader.get_handled_count() == 5


async def test_is_empty_does_not_depend_on_fetch_next_request(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client)

    items = []

    for _ in range(5):
        item = await sitemap_loader.fetch_next_request()
        assert item is not None
        assert not await sitemap_loader.is_finished()
        items.append(item)

    assert await sitemap_loader.is_empty()
    assert not await sitemap_loader.is_finished()

    for item in items:
        await sitemap_loader.mark_request_as_handled(item)

    assert await sitemap_loader.is_empty()
    assert await sitemap_loader.is_finished()


async def test_abort_sitemap_loading(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], max_buffer_size=2, http_client=http_client)

    item = await sitemap_loader.fetch_next_request()
    assert item is not None
    await sitemap_loader.mark_request_as_handled(item)

    assert not await sitemap_loader.is_empty()
    assert not await sitemap_loader.is_finished()

    await sitemap_loader.abort_loading()

    item = await sitemap_loader.fetch_next_request()
    assert item is not None
    await sitemap_loader.mark_request_as_handled(item)

    assert await sitemap_loader.is_finished()
