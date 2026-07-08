from __future__ import annotations

from contextlib import AsyncExitStack
from typing import TYPE_CHECKING

from crawlee._request import RequestState
from crawlee._utils.docs import docs_group
from crawlee.crawlers._abstract_http import AbstractHttpCrawler
from crawlee.crawlers._abstract_http._http_crawling_context import HttpCrawlingContext
from crawlee.crawlers._basic import ContextPipeline
from crawlee.crawlers._http._http_parser import NoParser

from ._file_download_crawling_context import FileDownloadCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from typing_extensions import Unpack

    from crawlee._types import BasicCrawlingContext
    from crawlee.crawlers._abstract_http import HttpCrawlerOptions


@docs_group('Crawlers')
class FileDownloadCrawler(AbstractHttpCrawler[FileDownloadCrawlingContext, bytes, bytes]):
    """A crawler for downloading files with plain HTTP requests.

    The `FileDownloadCrawler` builds on top of the `AbstractHttpCrawler`, which means it inherits all of its
    features. Unlike the other HTTP crawlers, it does not parse the response body. Any content type is accepted
    as-is, which makes it suitable for downloading binary files such as PDFs, images, videos or archives.

    By default the whole response body is buffered in memory and available via
    `await context.http_response.read()`. For large files, construct the crawler with `stream=True`. The request
    handler then receives a response whose body has not been read yet and can consume it in chunks via
    `context.http_response.read_stream()`. The connection stays open while the handler runs and is closed
    afterwards. A handler that returns without consuming the body just skips the download.

    ### Usage

    ```python
    from crawlee.crawlers import FileDownloadCrawler, FileDownloadCrawlingContext

    crawler = FileDownloadCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        context.log.info(f'Downloading {context.request.url} ...')

        # Read the whole file and save it to the key-value store.
        content = await context.http_response.read()
        kvs = await context.get_key_value_store()
        await kvs.set_value(
            key=context.request.url.split('/')[-1],
            value=content,
            content_type=context.http_response.headers.get('content-type'),
        )

    await crawler.run(['https://example.com/document.pdf'])
    ```

    With `stream=True`, the body can be written to a file in chunks without loading it into memory:

    ```python
    from pathlib import Path

    from crawlee.crawlers import FileDownloadCrawler, FileDownloadCrawlingContext

    crawler = FileDownloadCrawler(stream=True)

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        file_name = context.request.url.split('/')[-1]

        # Write chunks to disk as they arrive.
        with Path(file_name).open('wb') as file:
            async for chunk in context.http_response.read_stream():
                file.write(chunk)

    await crawler.run(['https://example.com/large-video.mp4'])
    ```
    """

    def __init__(
        self,
        *,
        stream: bool = False,
        **kwargs: Unpack[HttpCrawlerOptions[FileDownloadCrawlingContext]],
    ) -> None:
        """Initialize a new instance.

        Args:
            stream: Whether to stream response bodies instead of buffering them in memory. In stream mode
                the request handler consumes the body via `context.http_response.read_stream()`.
            kwargs: Additional keyword arguments to pass to the underlying `AbstractHttpCrawler`.
        """
        self._stream_response_body = stream

        kwargs['_context_pipeline'] = self._create_file_download_pipeline()

        super().__init__(
            parser=NoParser(),
            **kwargs,
        )

    def _create_file_download_pipeline(self) -> ContextPipeline[FileDownloadCrawlingContext]:
        """Create the file download context pipeline with expected pipeline steps."""
        make_http_request = self._make_http_stream_request if self._stream_response_body else self._make_http_request

        return (
            ContextPipeline()
            .compose(self._manage_shared_navigation_timeout)
            .compose(self._execute_pre_navigation_hooks)
            .compose(make_http_request)
            .compose(self._execute_post_navigation_hooks)
            .compose(self._handle_status_code_response)
            .compose(self._to_file_download_crawling_context)
        )

    async def _make_http_stream_request(
        self, context: BasicCrawlingContext
    ) -> AsyncGenerator[HttpCrawlingContext, None]:
        """Make a streamed HTTP request and create context enhanced by the streamed HTTP response.

        Args:
            context: The current crawling context.

        Yields:
            The original crawling context enhanced by the streamed HTTP response.
        """
        async with AsyncExitStack() as exit_stack:
            async with self._shared_navigation_timeouts[id(context.request)] as remaining_timeout:
                response = await exit_stack.enter_async_context(
                    self._http_client.stream(
                        url=context.request.url,
                        method=context.request.method,
                        headers=context.request.headers,
                        payload=context.request.payload,
                        session=context.session,
                        proxy_info=context.proxy_info,
                        timeout=remaining_timeout,
                    )
                )

            self._statistics.register_status_code(response.status_code)
            context.request.state = RequestState.AFTER_NAV

            yield HttpCrawlingContext.from_basic_crawling_context(context=context, http_response=response)

    async def _to_file_download_crawling_context(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[FileDownloadCrawlingContext, None]:
        """Convert the HTTP crawling context to the final file download crawling context."""
        yield FileDownloadCrawlingContext.from_http_crawling_context(context)
