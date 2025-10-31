import asyncio
import base64
import gzip

from yarl import URL

from crawlee import RequestOptions, RequestTransformAction
from crawlee.http_clients._base import HttpClient
from crawlee.request_loaders._sitemap_request_loader import SitemapRequestLoader
from crawlee.storages import KeyValueStore

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


async def test_create_persist_state_for_sitemap_loading(
    server_url: URL, http_client: HttpClient, key_value_store: KeyValueStore
) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    persist_key = 'create_persist_state'
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, persist_state_key=persist_key)
    assert await sitemap_loader.is_finished() is False

    await sitemap_loader.close()

    state_data = await key_value_store.get_value(persist_key)

    assert state_data is not None
    assert state_data['handledCount'] == 0


async def test_data_persistence_for_sitemap_loading(
    server_url: URL, http_client: HttpClient, key_value_store: KeyValueStore
) -> None:
    async def wait_for_sitemap_loader_not_empty(sitemap_loader: SitemapRequestLoader) -> None:
        while await sitemap_loader.is_empty() and not await sitemap_loader.is_finished():  # noqa: ASYNC110
            await asyncio.sleep(0.1)

    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    persist_key = 'data_persist_state'
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, persist_state_key=persist_key)

    # Give time to load
    await asyncio.wait_for(wait_for_sitemap_loader_not_empty(sitemap_loader), timeout=2)

    await sitemap_loader.close()

    state_data = await key_value_store.get_value(persist_key)

    assert state_data is not None
    assert state_data['handledCount'] == 0
    assert state_data['totalCount'] == 5
    assert len(state_data['urlQueue']) == 5


async def test_recovery_data_persistence_for_sitemap_loading(
    server_url: URL, http_client: HttpClient, key_value_store: KeyValueStore
) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    persist_key = 'recovery_persist_state'
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, persist_state_key=persist_key)

    item = await sitemap_loader.fetch_next_request()

    assert item is not None
    await sitemap_loader.mark_request_as_handled(item)

    await sitemap_loader.close()

    state_data = await key_value_store.get_value(persist_key)

    assert state_data is not None
    next_item_in_kvs = state_data['urlQueue'][0]

    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, persist_state_key=persist_key)

    item = await sitemap_loader.fetch_next_request()

    assert item is not None
    assert item.url == next_item_in_kvs


async def test_transform_request_function(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))

    def transform_request(request_options: RequestOptions) -> RequestOptions | RequestTransformAction:
        request_options['user_data'] = {'transformed': True}
        return request_options

    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)],
        http_client=http_client,
        transform_request_function=transform_request,
    )

    extracted_urls = set()

    while not await sitemap_loader.is_finished():
        request = await sitemap_loader.fetch_next_request()
        assert request is not None
        assert request.user_data.get('transformed') is True

        extracted_urls.add(request.url)

        await sitemap_loader.mark_request_as_handled(request)

    assert len(extracted_urls) == 5
    assert extracted_urls == {
        'http://not-exists.com/',
        'http://not-exists.com/catalog?item=12&desc=vacation_hawaii',
        'http://not-exists.com/catalog?item=73&desc=vacation_new_zealand',
        'http://not-exists.com/catalog?item=74&desc=vacation_newfoundland',
        'http://not-exists.com/catalog?item=83&desc=vacation_usa',
    }
