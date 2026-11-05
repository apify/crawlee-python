from __future__ import annotations

import base64
import gzip
from datetime import datetime
from typing import TYPE_CHECKING

from crawlee._utils.sitemap import (
    Sitemap,
    SitemapUrl,
    parse_sitemap,
)
from tests._utils import (
    get_basic_results,
    get_basic_sitemap,
)

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.http_clients._base import HttpClient


def compress_gzip(data: str) -> bytes:
    """Compress a string using gzip."""
    return gzip.compress(data.encode())


def encode_base64(data: bytes) -> str:
    """Encode bytes to a base64 string."""
    return base64.b64encode(data).decode('utf-8')


async def test_sitemap(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a basic sitemap."""
    sitemap_url = (server_url / 'sitemap.xml').with_query(
        base64=encode_base64(get_basic_sitemap(url=server_url).encode()), c_type='application/xml; charset=utf-8'
    )
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 5
    assert set(sitemap.urls) == get_basic_results(server_url)


async def test_sitemap_different_url(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a basic sitemap when sitemap contains links to different url. Those should be ignored."""
    different_url = 'https://other.com/'
    sitemap_url = (server_url / 'sitemap.xml').with_query(
        base64=encode_base64(get_basic_sitemap(url=different_url).encode()), c_type='application/xml; charset=utf-8'
    )
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 0


async def test_sitemap_different_url_allowed(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a basic sitemap when sitemap contains links to different url, and it is explicitly allowed."""
    different_url = 'https://other.com/'
    sitemap_url = (server_url / 'sitemap.xml').with_query(
        base64=encode_base64(get_basic_sitemap(url=different_url).encode()), c_type='application/xml; charset=utf-8'
    )
    sitemap = await Sitemap.load(
        str(sitemap_url), http_client=http_client, parse_sitemap_options={'enqueue_strategy': 'all'}
    )

    assert len(sitemap.urls) == 5
    assert set(sitemap.urls) == get_basic_results(different_url)


async def test_extract_metadata_sitemap(server_url: URL, http_client: HttpClient) -> None:
    """Test extracting item metadata from a sitemap."""
    sitemap_url = (server_url / 'sitemap.xml').with_query(
        base64=encode_base64(get_basic_sitemap(url=server_url).encode()), c_type='application/xml; charset=utf-8'
    )

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': str(sitemap_url)}], http_client=http_client)]
    assert len(items) == 5
    assert items[0] == SitemapUrl(
        loc=str(server_url),
        priority=0.8,
        changefreq='monthly',
        lastmod=datetime.fromisoformat('2005-02-03'),
        origin_sitemap_url=str(sitemap_url),
    )


async def test_gzipped_sitemap(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a gzipped sitemap with correct type and .xml.gz url."""
    gzipped_data = encode_base64(compress_gzip(get_basic_sitemap(url=server_url)))
    sitemap_url = (server_url / 'sitemap.xml.gz').with_query(base64=gzipped_data, c_type='application/gzip')
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)
    assert len(sitemap.urls) == 5
    assert set(sitemap.urls) == get_basic_results(server_url)


async def test_gzipped_sitemap_with_invalid_data(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a invalid gzipped sitemap with correct type and .xml.gz url."""
    compress_data = compress_gzip(get_basic_sitemap(url=server_url))
    invalid_gzipped_data = encode_base64(compress_data[:30])
    sitemap_url = (server_url / 'sitemap.xml.gz').with_query(base64=invalid_gzipped_data, c_type='application/gzip')
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 0
    assert sitemap.urls == []


async def test_gz_sitemap_with_non_gzipped(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a sitemap with gzip type and .xml.gz url, but without gzipped data."""
    sitemap_url = (server_url / 'sitemap.xml.gz').with_query(
        base64=encode_base64(get_basic_sitemap(url=server_url).encode()), c_type='application/gzip'
    )
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 5
    assert set(sitemap.urls) == get_basic_results(server_url)


async def test_gzipped_sitemap_with_bad_type(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a gzipped sitemap with bad type and .xml.gz url."""
    gzipped_data = encode_base64(compress_gzip(get_basic_sitemap(url=server_url)))
    sitemap_url = (server_url / 'sitemap.xml.gz').with_query(
        base64=gzipped_data, c_type='application/xml; charset=utf-8'
    )
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 5
    assert set(sitemap.urls) == get_basic_results(server_url)


async def test_xml_sitemap_with_gzipped_data(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a gzipped sitemap with correct type and .xml url."""
    gzipped_data = encode_base64(compress_gzip(get_basic_sitemap(url=server_url)))
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=gzipped_data, c_type='application/gzip')
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 5
    assert set(sitemap.urls) == get_basic_results(server_url)


async def test_parent_sitemap(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a parent sitemap that references child sitemaps."""
    parent_sitemap = """
<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<sitemap>
<loc>{child_sitemap}</loc>
<lastmod>2004-12-23</lastmod>
</sitemap>
<sitemap>
<loc>{child_sitemap_2}</loc>
<lastmod>2004-12-23</lastmod>
</sitemap>
</sitemapindex>
""".strip()
    child_sitemap = (server_url / 'sitemap.xml').with_query(
        base64=encode_base64(get_basic_sitemap(url=server_url).encode())
    )
    child_sitemap_2 = (server_url / 'sitemap.xml.gz').with_query(
        base64=encode_base64(compress_gzip(get_basic_sitemap(url=server_url)))
    )
    parent_sitemap_content = parent_sitemap.format(child_sitemap=child_sitemap, child_sitemap_2=child_sitemap_2)
    encoded_parent_sitemap_content = encode_base64(parent_sitemap_content.encode())
    parent_sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encoded_parent_sitemap_content)

    sitemap = await Sitemap.load(str(parent_sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 10
    assert set(sitemap.urls) == get_basic_results(server_url)


async def test_non_sitemap_url(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a URL that does not point to a sitemap."""
    sitemap = await Sitemap.load(str(server_url), http_client=http_client)

    assert len(sitemap.urls) == 0
    assert sitemap.urls == []


async def test_cdata_sitemap(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a sitemap with CDATA sections."""
    cdata_sitemap = f"""
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url>
<loc><![CDATA[{server_url}catalog]]></loc>
</url>
</urlset>
    """.strip()
    sitemap_url = (server_url / 'sitemap.xml').with_query(
        base64=encode_base64(cdata_sitemap.encode()), c_type='application/xml; charset=utf-8'
    )
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 1
    assert sitemap.urls == [f'{server_url}catalog']


async def test_txt_sitemap(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a plain text sitemap."""
    urls = [
        f'{server_url}catalog?item=78&desc=vacation_crete',
        f'{server_url}catalog?item=79&desc=vacation_somalia',
    ]
    txt_sitemap_content = '\n'.join(urls)

    sitemap_url = (server_url / 'sitemap.txt').with_query(base64=encode_base64(txt_sitemap_content.encode()))
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 2
    assert set(sitemap.urls) == {
        f'{server_url}catalog?item=78&desc=vacation_crete',
        f'{server_url}catalog?item=79&desc=vacation_somalia',
    }


async def test_sitemap_pretty(server_url: URL, http_client: HttpClient) -> None:
    """Test loading a pretty-printed sitemap."""
    pretty_sitemap = f"""
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url>
<loc>
    {server_url}catalog?item=80&amp;desc=vacation_turkey
</loc>
<lastmod>
    2005-02-03
</lastmod>
<changefreq>

    monthly
</changefreq>
<priority>
    0.8
</priority>
</url>
</urlset>
""".strip()
    sitemap_url = (server_url / 'sitemap.xml').with_query(
        base64=encode_base64(pretty_sitemap.encode()), c_type='application/xml; charset=utf-8'
    )
    sitemap = await Sitemap.load(str(sitemap_url), http_client=http_client)

    assert len(sitemap.urls) == 1
    assert sitemap.urls == [f'{server_url}catalog?item=80&desc=vacation_turkey']
