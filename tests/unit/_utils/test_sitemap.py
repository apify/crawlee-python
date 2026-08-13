import base64
import gzip
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from xml.sax.expatreader import ExpatParser

import pytest
from yarl import URL

from crawlee._utils.sitemap import (
    DEFAULT_MAX_DEPTH,
    ParseSitemapOptions,
    Sitemap,
    SitemapSource,
    SitemapUrl,
    _TxtSitemapParser,
    _XMLSaxSitemapHandler,
    _XmlSitemapParser,
    discover_valid_sitemaps,
    parse_sitemap,
)
from crawlee.http_clients._base import HttpClient, HttpResponse
from tests.unit.utils import (
    DEFAULT_URL,
    get_basic_results,
    get_basic_sitemap,
    make_status_stream_client,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable


def _make_mock_client(url_map: dict[str, tuple[int, bytes]]) -> AsyncMock:
    async def send_request(url: str, **_kwargs: Any) -> HttpResponse:
        status, body = 404, b''
        for pattern, (s, b) in url_map.items():
            if pattern in url:
                status, body = s, b
                break
        response = MagicMock(spec=HttpResponse)
        response.status_code = status
        response.read = AsyncMock(return_value=body)
        return response

    client = AsyncMock(spec=HttpClient)
    client.send_request.side_effect = send_request
    return client


def _make_flaky_stream_client(body: bytes, *, fail_times: int) -> tuple[AsyncMock, list[int]]:
    """Create a mock client whose `stream` fails with a network error for the first `fail_times` calls."""
    attempts: list[int] = []

    @asynccontextmanager
    async def stream(_url: str, **_kwargs: Any) -> 'AsyncIterator[HttpResponse]':
        attempt = len(attempts) + 1
        attempts.append(attempt)
        if attempt <= fail_times:
            raise ConnectionError(f'Network error on attempt {attempt}')

        async def read_stream() -> 'AsyncIterator[bytes]':
            yield body

        response = MagicMock(spec=HttpResponse)
        response.status_code = 200
        response.headers = {'content-type': 'application/xml; charset=utf-8'}
        response.read_stream = read_stream
        yield cast('HttpResponse', response)

    client = AsyncMock(spec=HttpClient)
    client.stream = stream
    return client, attempts


def _make_stream_client(body_for_url: 'Callable[[str], bytes]') -> tuple[AsyncMock, list[str]]:
    """Create a mock client whose `stream` serves `body_for_url(url)` in a single chunk and records fetched URLs."""
    fetched: list[str] = []

    @asynccontextmanager
    async def stream(url: str, **_kwargs: Any) -> 'AsyncIterator[HttpResponse]':
        fetched.append(url)

        async def read_stream() -> 'AsyncIterator[bytes]':
            yield body_for_url(url)

        response = MagicMock(spec=HttpResponse)
        response.status_code = 200
        response.headers = {'content-type': 'application/xml; charset=utf-8'}
        response.read_stream = read_stream
        yield cast('HttpResponse', response)

    client = AsyncMock(spec=HttpClient)
    client.stream = stream
    return client, fetched


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


async def test_sitemap_from_string() -> None:
    """Test creating a Sitemap instance from an XML string."""
    sitemap = await Sitemap.from_xml_string(get_basic_sitemap())

    assert len(sitemap.urls) == 5
    assert set(sitemap.urls) == get_basic_results()


async def test_malformed_sitemap_keeps_urls() -> None:
    """A parse error must not discard the URLs collected before it."""
    malformed = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f'<url><loc>{DEFAULT_URL}first</loc></url>\n'
        f'<url><loc>{DEFAULT_URL}second</loc></url>\n'
        f'<url><loc>{DEFAULT_URL}third</loc></mismatched>\n'
        '</urlset>'
    )

    sitemap = await Sitemap.from_xml_string(malformed)

    assert sitemap.urls == [f'{DEFAULT_URL}first', f'{DEFAULT_URL}second']


async def test_sitemap_fetch_retries_on_transient_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transient fetch errors are retried up to `sitemap_retries` times before giving up."""
    monkeypatch.setattr('crawlee._utils.sitemap._sleep_before_sitemap_retry', AsyncMock())
    client, attempts = _make_flaky_stream_client(get_basic_sitemap().encode(), fail_times=2)

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': f'{DEFAULT_URL}sitemap.xml'}], client)]

    assert len(attempts) == 3
    assert {item.loc for item in items} == get_basic_results()


async def test_sitemap_fetch_raises_after_retries_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    """A persistent fetch error is raised to the caller once all retries are exhausted."""
    monkeypatch.setattr('crawlee._utils.sitemap._sleep_before_sitemap_retry', AsyncMock())
    client, attempts = _make_flaky_stream_client(get_basic_sitemap().encode(), fail_times=10)

    with pytest.raises(ConnectionError):
        _ = [item async for item in parse_sitemap([{'type': 'url', 'url': f'{DEFAULT_URL}sitemap.xml'}], client)]

    assert len(attempts) == 3


async def test_sitemap_fetch_retries_retryable_http_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retryable HTTP errors are retried before parsing a successful response."""
    monkeypatch.setattr('crawlee._utils.sitemap._sleep_before_sitemap_retry', AsyncMock())
    sitemap_url = f'{DEFAULT_URL}sitemap.xml'
    client, attempts = make_status_stream_client(
        {sitemap_url: [(503, b''), (503, b''), (200, get_basic_sitemap().encode())]}
    )

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': sitemap_url}], client)]

    assert attempts == [503, 503, 200]
    assert {item.loc for item in items} == get_basic_results()


async def test_sitemap_fetch_skips_http_error_after_retries_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    """A persistent retryable HTTP error is skipped once retries are exhausted."""
    monkeypatch.setattr('crawlee._utils.sitemap._sleep_before_sitemap_retry', AsyncMock())
    sitemap_url = f'{DEFAULT_URL}sitemap.xml'
    client, attempts = make_status_stream_client({sitemap_url: [(503, b'')]})

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': sitemap_url}], client)]

    assert attempts == [503, 503, 503]
    assert items == []


async def test_sitemap_fetch_does_not_retry_terminal_http_status() -> None:
    """Terminal HTTP errors are skipped without parsing their response body or retrying."""
    sitemap_url = f'{DEFAULT_URL}sitemap.xml'
    client, attempts = make_status_stream_client({sitemap_url: [(404, get_basic_sitemap().encode())]})

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': sitemap_url}], client)]

    assert attempts == [404]
    assert items == []


async def test_sitemap_fetch_skips_redirect_without_retry() -> None:
    """Redirect responses that reach the parser are skipped without retrying."""
    sitemap_url = f'{DEFAULT_URL}sitemap.xml'
    client, attempts = make_status_stream_client({sitemap_url: [(302, get_basic_sitemap().encode())]})

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': sitemap_url}], client)]

    assert attempts == [302]
    assert items == []


async def test_sitemap_fetch_does_not_retry_too_many_requests() -> None:
    """429 is not retried on the session-less sitemap fetch path."""
    sitemap_url = f'{DEFAULT_URL}sitemap.xml'
    client, attempts = make_status_stream_client({sitemap_url: [(429, b'')]})

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': sitemap_url}], client)]

    assert attempts == [429]
    assert items == []


async def test_sitemap_partial_http_failure_keeps_healthy_source() -> None:
    """An HTTP failure in one source does not discard URLs from another source."""
    healthy_url = f'{DEFAULT_URL}sitemap.xml'
    missing_url = f'{DEFAULT_URL}missing.xml'
    client, attempts = make_status_stream_client(
        {
            healthy_url: [(200, get_basic_sitemap().encode())],
            missing_url: [(404, b'')],
        }
    )
    sources: list[SitemapSource] = [
        {'type': 'url', 'url': healthy_url},
        {'type': 'url', 'url': missing_url},
    ]

    items = [item async for item in parse_sitemap(sources, client)]

    assert attempts == [200, 404]
    assert {item.loc for item in items} == get_basic_results()


async def test_sitemap_partial_fetch_failure_keeps_healthy_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fetch exception in one source is suppressed when another source succeeds."""
    monkeypatch.setattr('crawlee._utils.sitemap._sleep_before_sitemap_retry', AsyncMock())
    client, attempts = _make_flaky_stream_client(get_basic_sitemap().encode(), fail_times=3)
    sources: list[SitemapSource] = [
        {'type': 'url', 'url': f'{DEFAULT_URL}broken.xml'},
        {'type': 'url', 'url': f'{DEFAULT_URL}sitemap.xml'},
    ]

    items = [item async for item in parse_sitemap(sources, client)]

    assert len(attempts) == 4
    assert {item.loc for item in items} == get_basic_results()


async def test_sitemap_skipped_source_does_not_hide_fetch_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A skipped HTTP source does not count as success, so a sibling fetch error still raises."""
    monkeypatch.setattr('crawlee._utils.sitemap._sleep_before_sitemap_retry', AsyncMock())
    missing_url = f'{DEFAULT_URL}missing.xml'
    broken_url = f'{DEFAULT_URL}broken.xml'
    client, attempts = make_status_stream_client(
        {
            missing_url: [(404, b'')],
            broken_url: [ConnectionError('Network error')],
        }
    )
    sources: list[SitemapSource] = [
        {'type': 'url', 'url': missing_url},
        {'type': 'url', 'url': broken_url},
    ]

    with pytest.raises(ConnectionError, match='Network error'):
        _ = [item async for item in parse_sitemap(sources, client)]

    assert attempts == [404]


async def test_sitemap_raises_first_source_error_when_all_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """If every source fails with a real error, the first error is raised."""
    monkeypatch.setattr('crawlee._utils.sitemap._sleep_before_sitemap_retry', AsyncMock())
    first_url = f'{DEFAULT_URL}first.xml'
    second_url = f'{DEFAULT_URL}second.xml'
    client, _ = make_status_stream_client(
        {
            first_url: [ConnectionError('first')],
            second_url: [OSError('second')],
        }
    )
    sources: list[SitemapSource] = [
        {'type': 'url', 'url': first_url},
        {'type': 'url', 'url': second_url},
    ]

    with pytest.raises(ConnectionError, match='first'):
        _ = [item async for item in parse_sitemap(sources, client)]


async def test_gzip_bomb_sitemap_truncated_at_size_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """A gzip sitemap inflating past the size cap is truncated instead of being decompressed without bound."""
    monkeypatch.setattr('crawlee._utils.sitemap.MAX_SITEMAP_SIZE', 64 * 1024)
    total_urls = 20_000
    locs = ''.join(f'<url><loc>{DEFAULT_URL}page-{i}</loc></url>' for i in range(total_urls))
    # ~1 MB of XML arriving as a single small compressed chunk.
    bomb = compress_gzip(f'<urlset>{locs}</urlset>')

    client, _ = _make_stream_client(lambda _url: bomb)
    items = [item async for item in parse_sitemap([{'type': 'url', 'url': f'{DEFAULT_URL}sitemap.xml'}], client)]

    assert 0 < len(items) < total_urls


async def test_gzip_sitemap_stops_reading_after_member_end() -> None:
    """Trailing bytes after a complete gzip member must not be read; otherwise they accumulate unbounded."""
    member = compress_gzip(get_basic_sitemap())
    chunks_read = 0

    @asynccontextmanager
    async def stream(_url: str, **_kwargs: Any) -> 'AsyncIterator[HttpResponse]':
        async def read_stream() -> 'AsyncIterator[bytes]':
            nonlocal chunks_read
            chunks_read += 1
            yield member
            # A malicious server would stream trailing junk forever; reading any of it is the bug.
            while True:
                chunks_read += 1
                yield b'\x00' * 65536

        response = MagicMock(spec=HttpResponse)
        response.status_code = 200
        response.headers = {'content-type': 'application/gzip'}
        response.read_stream = read_stream
        yield cast('HttpResponse', response)

    client = AsyncMock(spec=HttpClient)
    client.stream = stream

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': f'{DEFAULT_URL}sitemap.xml.gz'}], client)]

    assert {item.loc for item in items} == get_basic_results()
    # The member fits in the first chunk, so at most the first trailing chunk may be pulled before we stop.
    assert chunks_read <= 2


async def test_gzip_text_sitemap_parsed_by_extension_when_served_as_gzip() -> None:
    """A gzipped `.txt.gz` text sitemap served as `application/gzip` is parsed as text, not XML."""
    urls = {f'{DEFAULT_URL}page-{i}' for i in range(3)}
    body = compress_gzip('\n'.join(urls))

    @asynccontextmanager
    async def stream(_url: str, **_kwargs: Any) -> 'AsyncIterator[HttpResponse]':
        async def read_stream() -> 'AsyncIterator[bytes]':
            yield body

        response = MagicMock(spec=HttpResponse)
        response.status_code = 200
        response.headers = {'content-type': 'application/gzip'}
        response.read_stream = read_stream
        yield cast('HttpResponse', response)

    client = AsyncMock(spec=HttpClient)
    client.stream = stream

    items = [item async for item in parse_sitemap([{'type': 'url', 'url': f'{DEFAULT_URL}sitemap.txt.gz'}], client)]

    assert {item.loc for item in items} == urls


async def test_nested_sitemaps_followed_only_to_default_max_depth() -> None:
    """A chain of unique nested sitemap URLs is followed only up to the default max depth."""
    chain_length = 3 * DEFAULT_MAX_DEPTH

    def body_for_url(url: str) -> bytes:
        index = int(url.removesuffix('.xml').rsplit('-', 1)[-1])
        if index >= chain_length:
            return f'<urlset><url><loc>{DEFAULT_URL}page</loc></url></urlset>'.encode()
        next_sitemap = f'{DEFAULT_URL}sitemap-{index + 1}.xml'
        return f'<sitemapindex><sitemap><loc>{next_sitemap}</loc></sitemap></sitemapindex>'.encode()

    client, fetched = _make_stream_client(body_for_url)
    _ = [item async for item in parse_sitemap([{'type': 'url', 'url': f'{DEFAULT_URL}sitemap-0.xml'}], client)]

    assert len(fetched) == DEFAULT_MAX_DEPTH + 1


async def test_parse_sitemap_with_partial_options() -> None:
    """Test that missing keys in partial `ParseSitemapOptions` fall back to defaults."""
    options = ParseSitemapOptions(timeout=timedelta(seconds=10))
    items = [item async for item in parse_sitemap([{'type': 'raw', 'content': get_basic_sitemap()}], options=options)]

    assert {item.loc for item in items} == get_basic_results()


async def test_discover_sitemap_from_robots_txt() -> None:
    """Sitemap URL found in robots.txt is yielded."""
    robots_content = b'User-agent: *\nSitemap: http://example.com/custom-sitemap.xml'
    http_client = _make_mock_client({'robots.txt': (200, robots_content)})

    urls = [url async for url in discover_valid_sitemaps(['http://example.com/page'], http_client=http_client)]

    assert urls == ['http://example.com/custom-sitemap.xml']


async def test_discover_sitemap_from_common_paths() -> None:
    """Sitemap is found at common paths when robots.txt has none."""
    http_client = _make_mock_client(
        {'/sitemap.xml': (200, b''), '/sitemap.txt': (200, b''), '/sitemap_index.xml': (200, b'')}
    )

    urls = [url async for url in discover_valid_sitemaps(['http://example.com/page'], http_client=http_client)]

    assert urls == [
        'http://example.com/sitemap.xml',
        'http://example.com/sitemap.txt',
        'http://example.com/sitemap_index.xml',
    ]


async def test_discover_sitemap_from_input_url() -> None:
    """Input URL that is already a sitemap is yielded directly without checking common paths."""
    http_client = _make_mock_client({'/sitemap.txt': (200, b'')})

    urls = [url async for url in discover_valid_sitemaps(['http://example.com/sitemap.xml'], http_client=http_client)]

    assert urls == ['http://example.com/sitemap.xml']


async def test_discover_sitemap_deduplication() -> None:
    """Sitemap URL found in robots.txt is not yielded again from common paths check."""
    robots_content = b'User-agent: *\nSitemap: http://example.com/sitemap.xml'
    http_client = _make_mock_client(
        {
            'robots.txt': (200, robots_content),
            '/sitemap.xml': (200, b''),
        }
    )

    urls = [url async for url in discover_valid_sitemaps(['http://example.com/page'], http_client=http_client)]

    assert urls == ['http://example.com/sitemap.xml']


async def test_discover_sitemaps_multiple_domains() -> None:
    """Sitemaps from multiple domains are all discovered."""
    http_client = _make_mock_client(
        {
            'domain-a.com/sitemap.xml': (200, b''),
            'domain-b.com/sitemap.xml': (200, b''),
        }
    )

    urls = [
        url
        async for url in discover_valid_sitemaps(
            ['http://domain-a.com/page', 'http://domain-b.com/page'],
            http_client=http_client,
        )
    ]

    assert set(urls) == {
        'http://domain-a.com/sitemap.xml',
        'http://domain-b.com/sitemap.xml',
    }


def test_xml_handler_resets_current_tag_on_end_element() -> None:
    """Closing a tracked tag resets the handler's current tag so stray text between elements is ignored."""
    handler = _XMLSaxSitemapHandler()
    handler.startElement('urlset', MagicMock())
    handler.startElement('url', MagicMock())
    handler.startElement('loc', MagicMock())
    handler.characters('https://example.com/')
    handler.endElement('loc')

    assert handler._current_tag is None

    # Stray text between elements must not be buffered.
    handler.characters('   stray text   ')
    assert handler._buffer == 'https://example.com/'


def test_xml_handler_discards_metadata_from_url_without_loc() -> None:
    """A <url> block with metadata but no <loc> must not leak its metadata into the next <url>."""
    handler = _XMLSaxSitemapHandler()
    parser = ExpatParser()
    parser.setContentHandler(handler)
    parser.feed(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        '<url><lastmod>2005-01-01</lastmod><priority>0.1</priority><changefreq>daily</changefreq></url>'
        '<url><loc>https://example.com/real-page</loc></url>'
        '</urlset>'
    )

    assert handler.items == [{'type': 'url', 'loc': 'https://example.com/real-page'}]


def test_xml_handler_discards_url_with_empty_loc() -> None:
    """A <url> block with an empty <loc></loc> must be discarded instead of emitting an empty-string URL."""
    handler = _XMLSaxSitemapHandler()
    parser = ExpatParser()
    parser.setContentHandler(handler)
    parser.feed(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        '<url><loc></loc><priority>0.5</priority></url>'
        '<url><loc>https://example.com/real-page</loc></url>'
        '</urlset>'
    )

    assert handler.items == [{'type': 'url', 'loc': 'https://example.com/real-page'}]


async def test_txt_parser_flush_clears_buffer() -> None:
    """Feeding more data after flush() must not concatenate the previously flushed URL."""
    parser = _TxtSitemapParser()
    items = [item async for item in parser.process_chunk('https://a.com/\nhttps://b.com/')]
    items += [item async for item in parser.flush()]
    items += [item async for item in parser.process_chunk('https://c.com/\n')]

    assert [item['loc'] for item in items] == ['https://a.com/', 'https://b.com/', 'https://c.com/']


async def test_xml_parser_skips_missing_flush(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A parser without `flush` is flushed silently, as on CPython before 3.10.14, 3.11.9 and 3.12.3."""
    monkeypatch.delattr(ExpatParser, 'flush', raising=False)
    parser = _XmlSitemapParser()

    with caplog.at_level(logging.WARNING, logger='crawlee._utils.sitemap'):
        items = [item async for item in parser.process_chunk(get_basic_sitemap())]
        items += [item async for item in parser.flush()]

    assert {item['loc'] for item in items} == get_basic_results()
    assert caplog.records == []


async def test_xml_parser_keeps_items_on_flush_error(caplog: pytest.LogCaptureFixture) -> None:
    """A failing `flush()` must not discard the items already collected."""
    parser = _XmlSitemapParser()
    parser._handler.items.append({'type': 'url', 'loc': f'{DEFAULT_URL}page'})

    # `create=True` covers interpreters where `ExpatParser` has no `flush` to replace.
    with (
        caplog.at_level(logging.WARNING, logger='crawlee._utils.sitemap'),
        patch.object(parser._parser, 'flush', side_effect=RuntimeError('Broken parser'), create=True),
    ):
        items = [item async for item in parser.flush()]

    assert items == [{'type': 'url', 'loc': f'{DEFAULT_URL}page'}]
    assert parser._handler.items == []
    assert 'Failed to parse remaining XML data: Broken parser' in caplog.text


async def test_discover_sitemap_url_without_host_skipped() -> None:
    """URLs without a host are skipped."""
    http_client = _make_mock_client({})

    urls = [url async for url in discover_valid_sitemaps(['not-a-valid-url'], http_client=http_client)]

    assert urls == []


async def test_raw_sitemap_index_processes_nested_sitemaps() -> None:
    """Test that nested sitemap respects source url."""
    raw_index = f"""
    <?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <sitemap>
    <loc>{DEFAULT_URL}child-sitemap.xml</loc>
    <lastmod>2004-12-23</lastmod>
    </sitemap>
    </sitemapindex>
    """.strip()

    # The child sitemap (same host as DEFAULT_URL) is fetched via the streaming client.
    client, _ = _make_flaky_stream_client(get_basic_sitemap().encode(), fail_times=0)

    items = [item async for item in parse_sitemap([{'type': 'raw', 'content': raw_index}], client)]

    assert {item.loc for item in items} == get_basic_results()
