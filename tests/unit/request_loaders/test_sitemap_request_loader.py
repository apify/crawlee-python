import asyncio
import base64
import gzip
from typing import TYPE_CHECKING
from unittest.mock import patch

from yarl import URL

from crawlee import RequestOptions, RequestTransformAction
from crawlee.http_clients._base import HttpClient
from crawlee.request_loaders._sitemap_request_loader import SitemapRequestLoader
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from crawlee._types import JsonSerializable

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
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, enqueue_strategy='all')

    while not await sitemap_loader.is_finished():
        item = await sitemap_loader.fetch_next_request()

        if item:
            await sitemap_loader.mark_request_as_handled(item)

    assert await sitemap_loader.is_empty()
    assert await sitemap_loader.is_finished()
    assert await sitemap_loader.get_total_count() == 5
    assert await sitemap_loader.get_handled_count() == 5


async def test_is_empty_does_not_depend_on_fetch_next_request(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, enqueue_strategy='all')

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

    await asyncio.sleep(0.1)

    assert await sitemap_loader.is_finished()


async def test_abort_sitemap_loading(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))
    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)], max_buffer_size=2, http_client=http_client, enqueue_strategy='all'
    )

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
    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)], http_client=http_client, persist_state_key=persist_key, enqueue_strategy='all'
    )
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
    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)], http_client=http_client, persist_state_key=persist_key, enqueue_strategy='all'
    )

    # Give time to load
    await asyncio.wait_for(wait_for_sitemap_loader_not_empty(sitemap_loader), timeout=10)

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
    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)], http_client=http_client, persist_state_key=persist_key, enqueue_strategy='all'
    )

    item = await sitemap_loader.fetch_next_request()

    assert item is not None
    await sitemap_loader.mark_request_as_handled(item)

    await sitemap_loader.close()

    state_data = await key_value_store.get_value(persist_key)

    assert state_data is not None
    next_item_in_kvs = state_data['urlQueue'][0]

    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)], http_client=http_client, persist_state_key=persist_key, enqueue_strategy='all'
    )

    item = await sitemap_loader.fetch_next_request()

    assert item is not None
    assert item.url == next_item_in_kvs


async def test_transform_request_function(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))

    def transform_request(request_options: RequestOptions) -> RequestOptions | RequestTransformAction:
        user_data: dict[str, JsonSerializable] = {'transformed': True}
        request_options['user_data'] = user_data
        return request_options

    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)],
        http_client=http_client,
        transform_request_function=transform_request,
        enqueue_strategy='all',
    )

    extracted_urls = set()

    while not await sitemap_loader.is_finished():
        request = await sitemap_loader.fetch_next_request()

        if request:
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


async def test_transform_request_function_with_skip(server_url: URL, http_client: HttpClient) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))

    def transform_request(_request_options: RequestOptions) -> RequestOptions | RequestTransformAction:
        return 'skip'

    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)],
        http_client=http_client,
        transform_request_function=transform_request,
        enqueue_strategy='all',
    )

    while not await sitemap_loader.is_finished():
        request = await sitemap_loader.fetch_next_request()

        if request:
            await sitemap_loader.mark_request_as_handled(request)

    # Even though the sitemap had URLs, all were skipped, so the loader should be empty and finished with
    # 0 handled requests.
    assert await sitemap_loader.is_empty()
    assert await sitemap_loader.is_finished()
    assert await sitemap_loader.get_total_count() == 0
    assert await sitemap_loader.get_handled_count() == 0


async def test_sitemap_loader_to_tandem(
    server_url: URL,
    http_client: HttpClient,
) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))

    sitemap_loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, enqueue_strategy='all')
    request_manager = await sitemap_loader.to_tandem()

    while not await sitemap_loader.is_finished():
        request = await request_manager.fetch_next_request()

        if request:
            await request_manager.mark_request_as_handled(request)

    assert await sitemap_loader.is_empty()
    assert await sitemap_loader.is_finished()

    assert await request_manager.is_empty()
    assert await request_manager.is_finished()


async def test_sitemap_loader_to_tandem_with_request_dropped(
    server_url: URL,
    http_client: HttpClient,
) -> None:
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(BASIC_SITEMAP.encode()))

    sitemap_loader = SitemapRequestLoader(
        [str(sitemap_url)],
        http_client=http_client,
        enqueue_strategy='all',
    )
    request_manager = await sitemap_loader.to_tandem()

    with patch.object(
        request_manager._read_write_manager, 'add_request', side_effect=Exception('Failed to add request')
    ):
        while not await sitemap_loader.is_finished():
            request = await request_manager.fetch_next_request()

            if request:
                await request_manager.mark_request_as_handled(request)

        assert await sitemap_loader.is_empty()
        assert await sitemap_loader.is_finished()

        assert await request_manager.is_empty()
        assert await request_manager.is_finished()


def _make_urlset(urls: list[str]) -> str:
    """Build a `<urlset>` sitemap XML containing the given URLs."""
    url_blocks = '\n'.join(f'<url><loc>{url}</loc></url>' for url in urls)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f'{url_blocks}\n'
        '</urlset>'
    )


def _make_sitemapindex(sitemap_urls: list[str]) -> str:
    """Build a `<sitemapindex>` XML pointing at the given nested sitemap URLs."""
    sitemap_blocks = '\n'.join(f'<sitemap><loc>{url}</loc></sitemap>' for url in sitemap_urls)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f'{sitemap_blocks}\n'
        '</sitemapindex>'
    )


async def test_sitemap_loader_filters_cross_host_urls(server_url: URL, http_client: HttpClient) -> None:
    """Default strategy `same-hostname` filters out content URLs that are not on the sitemap's host."""
    same_host_url = str(server_url / 'page')
    cross_host_url = 'http://other.test/payload'
    sitemap_content = _make_urlset([same_host_url, cross_host_url])
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(sitemap_content.encode()))

    loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client)

    fetched: list[str] = []
    while not await loader.is_finished():
        request = await loader.fetch_next_request()
        if request is not None:
            fetched.append(request.url)
            await loader.mark_request_as_handled(request)

    assert fetched == [same_host_url]


async def test_sitemap_loader_filters_cross_host_nested_sitemap(server_url: URL, http_client: HttpClient) -> None:
    """Nested `<sitemap><loc>` entries on a different host are dropped before fetching them."""
    child_content = _make_urlset([str(server_url / 'inner')])
    same_host_child_url = str((server_url / 'sitemap.xml').with_query(base64=encode_base64(child_content.encode())))
    cross_host_child_url = 'http://other.test/child.xml'
    index_content = _make_sitemapindex([same_host_child_url, cross_host_child_url])
    index_url = str((server_url / 'sitemap.xml').with_query(base64=encode_base64(index_content.encode())))

    loader = SitemapRequestLoader([index_url], http_client=http_client)

    fetched: list[str] = []
    while not await loader.is_finished():
        request = await loader.fetch_next_request()
        if request is not None:
            fetched.append(request.url)
            await loader.mark_request_as_handled(request)

    assert fetched == [str(server_url / 'inner')]


async def test_sitemap_loader_stamps_request_enqueue_strategy(server_url: URL, http_client: HttpClient) -> None:
    """Emitted `Request` objects carry the loader's enqueue strategy so redirects are policed downstream."""
    same_host_url = str(server_url / 'page')
    sitemap_content = _make_urlset([same_host_url])
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(sitemap_content.encode()))

    loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, enqueue_strategy='same-domain')
    request = await loader.fetch_next_request()
    assert request is not None
    assert request.enqueue_strategy == 'same-domain'
    await loader.mark_request_as_handled(request)


async def test_sitemap_loader_strategy_all_disables_filtering(server_url: URL, http_client: HttpClient) -> None:
    """Passing `enqueue_strategy='all'` keeps the pre-fix permissive behavior for opt-in callers."""
    cross_host_url = 'http://other.test/payload'
    sitemap_content = _make_urlset([cross_host_url])
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(sitemap_content.encode()))

    loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, enqueue_strategy='all')

    fetched: list[str] = []
    while not await loader.is_finished():
        request = await loader.fetch_next_request()
        if request is not None:
            fetched.append(request.url)
            await loader.mark_request_as_handled(request)

    assert fetched == [cross_host_url]


async def test_sitemap_loader_drops_non_http_scheme_under_strategy_all(
    server_url: URL, http_client: HttpClient
) -> None:
    """Even with `enqueue_strategy='all'`, sitemap entries with non-http(s) schemes are dropped."""
    http_url = 'http://other.test/page'
    sitemap_content = _make_urlset(
        [http_url, 'mailto:foo@bar.com', 'javascript:alert(1)', 'ftp://example.com/file.txt']
    )
    sitemap_url = (server_url / 'sitemap.xml').with_query(base64=encode_base64(sitemap_content.encode()))

    loader = SitemapRequestLoader([str(sitemap_url)], http_client=http_client, enqueue_strategy='all')

    fetched: list[str] = []
    while not await loader.is_finished():
        request = await loader.fetch_next_request()
        if request is not None:
            fetched.append(request.url)
            await loader.mark_request_as_handled(request)

    assert fetched == [http_url]
