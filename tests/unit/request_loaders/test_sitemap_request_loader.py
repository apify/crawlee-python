import gzip
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee._types import BasicCrawlingContext
from crawlee._utils.sitemap import DEFAULT_MAX_DEPTH
from crawlee.crawlers import BasicCrawler
from crawlee.http_clients._base import HttpClient, HttpResponse
from crawlee.request_loaders import RequestSourceStatus
from crawlee.request_loaders._sitemap_request_loader import SitemapRequestLoader
from tests._utils import (
    DEFAULT_URL,
    get_basic_results,
    get_basic_sitemap,
    make_status_stream_client,
    poll_until_condition,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def finished(readiness: RequestSourceStatus) -> bool:
    """Tell whether a loader reported that every request has been handled."""
    return readiness.status == 'finished'


def compress_gzip(data: str) -> bytes:
    """Compress a string using gzip."""
    return gzip.compress(data.encode())


async def test_nested_sitemap_chain_bounded_by_max_depth() -> None:
    """A malicious endless chain of unique nested sitemaps is followed only up to the default max depth."""
    fetched: list[str] = []

    async def send_request(url: str, **_kwargs: Any) -> HttpResponse:
        fetched.append(url)
        index = int(url.removesuffix('.xml').rsplit('-', 1)[-1])
        # Every sitemap is an index pointing at the next unique URL, so only the depth cap can stop the chain.
        next_sitemap = f'{DEFAULT_URL}sitemap-{index + 1}.xml'
        body = f'<sitemapindex><sitemap><loc>{next_sitemap}</loc></sitemap></sitemapindex>'.encode()

        async def read_stream() -> 'AsyncIterator[bytes]':
            yield body

        response = MagicMock(spec=HttpResponse)
        response.status_code = 200
        response.headers = {'content-type': 'application/xml; charset=utf-8'}
        response.read_stream = read_stream
        return cast('HttpResponse', response)

    client = AsyncMock(spec=HttpClient)
    client.send_request.side_effect = send_request

    loader = SitemapRequestLoader([f'{DEFAULT_URL}sitemap-0.xml'], http_client=client)
    assert finished(await poll_until_condition(loader.check_readiness, finished))

    assert len(fetched) == DEFAULT_MAX_DEPTH + 1


async def test_sitemap_http_error_is_retried_before_loading_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    """The loader retries transient HTTP errors and loads the eventual sitemap response."""
    monkeypatch.setattr('crawlee._utils.sitemap.SITEMAP_RETRY_DELAY', 0)
    sitemap_url = f'{DEFAULT_URL}sitemap.xml'
    client, attempts = make_status_stream_client(
        {sitemap_url: [(503, b''), (503, b''), (200, get_basic_sitemap().encode())]}
    )
    loader = SitemapRequestLoader([sitemap_url], http_client=client)

    while not finished(await loader.check_readiness()):
        request = await loader.fetch_next_request()
        if request:
            await loader.mark_request_as_handled(request)

    assert attempts == [503, 503, 200]
    assert await loader.get_total_count() == 5


async def test_sitemap_http_error_is_skipped_after_retries_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    """The loader finishes empty after an exhausted sitemap HTTP error."""
    monkeypatch.setattr('crawlee._utils.sitemap.SITEMAP_RETRY_DELAY', 0)
    sitemap_url = f'{DEFAULT_URL}sitemap.xml'
    client, attempts = make_status_stream_client({sitemap_url: [(503, b'')]})
    loader = SitemapRequestLoader([sitemap_url], http_client=client)

    assert await loader.fetch_next_request() is None

    assert attempts == [503, 503, 503]


async def test_sitemap_loader_drains_requests_after_later_source_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Already loaded URLs are still handed out after a later sitemap fails."""
    monkeypatch.setattr('crawlee._utils.sitemap.SITEMAP_RETRY_DELAY', 0)
    client, _ = make_status_stream_client(
        {
            f'{DEFAULT_URL}sitemap.xml': [(200, get_basic_sitemap().encode())],
            f'{DEFAULT_URL}broken.xml': [ConnectionError('Network error')],
        }
    )
    loader = SitemapRequestLoader(
        [f'{DEFAULT_URL}sitemap.xml', f'{DEFAULT_URL}broken.xml'], http_client=client, max_buffer_size=10
    )

    requests = []
    for _ in range(5):
        request = await loader.fetch_next_request()
        assert request is not None
        requests.append(request)

    assert not finished(await loader.check_readiness())
    for request in requests:
        await loader.mark_request_as_handled(request)

    assert finished(await poll_until_condition(loader.check_readiness, finished))


async def test_sitemap_loader_continues_after_earlier_source_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sitemap that fails to load is skipped and the remaining pending sitemaps are still loaded."""
    monkeypatch.setattr('crawlee._utils.sitemap.SITEMAP_RETRY_DELAY', 0)
    client, attempts = make_status_stream_client(
        {
            f'{DEFAULT_URL}broken.xml': [ConnectionError('Network error')],
            f'{DEFAULT_URL}sitemap.xml': [(200, get_basic_sitemap().encode())],
        }
    )
    loader = SitemapRequestLoader([f'{DEFAULT_URL}broken.xml', f'{DEFAULT_URL}sitemap.xml'], http_client=client)

    urls = []
    while not finished(await loader.check_readiness()):
        request = await loader.fetch_next_request()
        if request:
            urls.append(request.url)
            await loader.mark_request_as_handled(request)

    assert attempts == [200]
    assert set(urls) == get_basic_results()
    assert await loader.get_total_count() == 5


async def test_crawler_tandem_continues_when_sitemap_is_dead(monkeypatch: pytest.MonkeyPatch) -> None:
    """A dead sitemap plus seeded requests still crawls the seeded work."""
    monkeypatch.setattr('crawlee._utils.sitemap.SITEMAP_RETRY_DELAY', 0)
    broken_url = f'{DEFAULT_URL}broken.xml'
    client, _ = make_status_stream_client({broken_url: [ConnectionError('Network error')]})
    loader = SitemapRequestLoader([broken_url], http_client=client)
    request_manager = await loader.to_tandem()

    crawler = BasicCrawler(request_manager=request_manager)
    visited: list[str] = []

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        visited.append(context.request.url)

    stats = await crawler.run([f'{DEFAULT_URL}seeded-a', f'{DEFAULT_URL}seeded-b'])

    assert set(visited) == {f'{DEFAULT_URL}seeded-a', f'{DEFAULT_URL}seeded-b'}
    assert stats.requests_finished == 2
    assert stats.requests_failed == 0
