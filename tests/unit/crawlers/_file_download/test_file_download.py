from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock, call

import pytest

from crawlee.crawlers import FileDownloadCrawler, FileDownloadCrawlingContext
from tests.unit.server import generate_file_content

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.crawlers import HttpCrawlingContext
    from crawlee.http_clients._base import HttpClient

FILE_SIZE = 256 * 1024
CHUNK_SIZE = 16 * 1024


async def test_read_file(server_url: URL, http_client: HttpClient) -> None:
    """The whole file is read into memory."""
    crawler = FileDownloadCrawler(http_client=http_client)
    handler_call = Mock()

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        handler_call(
            content_type=context.http_response.headers.get('content-type'),
            body=await context.http_response.read(),
        )

    stats = await crawler.run([str(server_url.with_path('file').with_query(size=FILE_SIZE))])

    assert stats.requests_failed == 0
    assert stats.requests_finished == 1
    handler_call.assert_called_once()
    assert handler_call.call_args.kwargs['content_type'] == 'application/octet-stream'
    assert handler_call.call_args.kwargs['body'] == generate_file_content(FILE_SIZE)


async def test_stream_file(server_url: URL, http_client: HttpClient) -> None:
    """The body is read in chunks, with metadata available before consumption."""
    crawler = FileDownloadCrawler(stream=True, http_client=http_client)
    handler_call = Mock()

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        # Metadata is passed before the body argument, so it is read before the stream is consumed.
        handler_call(
            status_code=context.http_response.status_code,
            content_type=context.http_response.headers.get('content-type'),
            content_length=context.http_response.headers.get('content-length'),
            body=b''.join([chunk async for chunk in context.http_response.read_stream()]),
        )

    stats = await crawler.run([str(server_url.with_path('file').with_query(size=FILE_SIZE, chunk_size=CHUNK_SIZE))])

    assert stats.requests_failed == 0
    assert stats.requests_finished == 1
    handler_call.assert_called_once()
    assert handler_call.call_args.kwargs['status_code'] == 200
    assert handler_call.call_args.kwargs['content_type'] == 'application/octet-stream'
    assert handler_call.call_args.kwargs['content_length'] == str(FILE_SIZE)
    assert handler_call.call_args.kwargs['body'] == generate_file_content(FILE_SIZE)


async def test_slow_download(server_url: URL) -> None:
    """Chunked responses with pauses between chunks download fully."""
    crawler = FileDownloadCrawler(stream=True)
    handler_call = Mock()

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        handler_call(body=b''.join([chunk async for chunk in context.http_response.read_stream()]))

    url = server_url.with_path('file').with_query(size=8 * 1024, chunk_size=1024, throttle=0.2)
    stats = await crawler.run([str(url)])

    assert stats.requests_failed == 0
    handler_call.assert_called_once()
    assert handler_call.call_args.kwargs['body'] == generate_file_content(8 * 1024)


async def test_unconsumed_body(server_url: URL, http_client: HttpClient) -> None:
    """A handler that never reads the body finishes cleanly instead of hanging."""
    crawler = FileDownloadCrawler(stream=True, http_client=http_client)
    handler_call = Mock()

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        handler_call(status_code=context.http_response.status_code)

    stats = await crawler.run([str(server_url.with_path('file').with_query(size=FILE_SIZE))])

    assert stats.requests_failed == 0
    assert stats.requests_finished == 1
    handler_call.assert_called_once()
    assert handler_call.call_args.kwargs['status_code'] == 200


async def test_partially_consumed_body(server_url: URL, http_client: HttpClient) -> None:
    """A handler that stops reading early finishes without a retry."""
    crawler = FileDownloadCrawler(stream=True, http_client=http_client)
    handler_call = Mock()

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        handler_call()
        async for _ in context.http_response.read_stream():
            break

    stats = await crawler.run([str(server_url.with_path('file').with_query(size=FILE_SIZE, chunk_size=CHUNK_SIZE))])

    assert stats.requests_failed == 0
    assert stats.requests_finished == 1
    handler_call.assert_called_once()


async def test_retry_failed_download(server_url: URL, http_client: HttpClient) -> None:
    """A mid-stream failure retries and gets a fresh, complete response."""
    crawler = FileDownloadCrawler(stream=True, http_client=http_client, max_request_retries=1)
    handler_call = Mock()

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        stream = context.http_response.read_stream()
        # Fail mid-stream on the first attempt, then read the full body on the retry.
        if handler_call.call_count == 0:
            await anext(stream)
            handler_call(body=None)
            raise RuntimeError('Simulated handler failure')
        handler_call(body=b''.join([chunk async for chunk in stream]))

    stats = await crawler.run([str(server_url.with_path('file').with_query(size=FILE_SIZE, chunk_size=CHUNK_SIZE))])

    assert stats.requests_failed == 0
    assert stats.requests_finished == 1
    assert handler_call.call_count == 2
    assert handler_call.call_args.kwargs['body'] == generate_file_content(FILE_SIZE)


@pytest.mark.parametrize('stream', [False, True], ids=['buffered', 'stream'])
async def test_error_status_code(server_url: URL, *, stream: bool) -> None:
    """An error status code fails the request without calling the handler."""
    handler = AsyncMock()
    crawler = FileDownloadCrawler(stream=stream, max_request_retries=0, request_handler=handler)

    stats = await crawler.run([str(server_url / 'status/404')])

    assert stats.requests_failed == 1
    handler.assert_not_called()


@pytest.mark.parametrize('content_type', ['application/pdf', 'image/png', 'video/mp4'])
async def test_any_content_type(server_url: URL, content_type: str) -> None:
    """Any content type is downloaded as-is."""
    crawler = FileDownloadCrawler()
    handler_call = Mock()

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        handler_call(
            content_type=context.http_response.headers.get('content-type'),
            body=await context.http_response.read(),
        )

    stats = await crawler.run([str(server_url.with_path('file').with_query(size=1024, content_type=content_type))])

    assert stats.requests_failed == 0
    handler_call.assert_called_once()
    assert handler_call.call_args.kwargs['content_type'] == content_type
    assert handler_call.call_args.kwargs['body'] == generate_file_content(1024)


async def test_navigation_hooks(server_url: URL) -> None:
    """Navigation hooks run around a streamed request, before the handler."""
    crawler = FileDownloadCrawler(stream=True)
    tracker = Mock()

    @crawler.pre_navigation_hook
    async def pre_nav_hook(_context: object) -> None:
        tracker('pre_navigation')

    @crawler.post_navigation_hook
    async def post_nav_hook(context: HttpCrawlingContext) -> None:
        # The streamed response is already open when post-navigation hooks run.
        tracker('post_navigation', context.http_response.status_code)

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        tracker('handler')
        async for _ in context.http_response.read_stream():
            pass

    stats = await crawler.run([str(server_url.with_path('file').with_query(size=1024))])

    assert stats.requests_failed == 0
    assert tracker.mock_calls == [
        call('pre_navigation'),
        call('post_navigation', 200),
        call('handler'),
    ]
