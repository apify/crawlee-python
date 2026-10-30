from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee.crawlers._abstract_http import AbstractHttpCrawler
from crawlee.crawlers._basic import ContextPipeline
from crawlee.crawlers._http._http_parser import NoParser

from ._file_download_crawling_context import FileDownloadCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import Unpack

    from crawlee.crawlers._abstract_http import HttpCrawlerOptions
    from crawlee.crawlers._abstract_http._http_crawling_context import HttpCrawlingContext


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
    afterwards. A handler that returns without consuming the body just skips the download. In both modes, reading
    the body counts towards `navigation_timeout`, so raise it for long downloads.

    ### Usage

    ```python
    from yarl import URL

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
            key=URL(context.request.url).name,
            value=content,
            content_type=context.http_response.headers.get('content-type'),
        )

    await crawler.run(['https://example.com/document.pdf'])
    ```

    With `stream=True`, the body can be written to a file in chunks without loading it into memory:

    ```python
    from pathlib import Path

    from yarl import URL

    from crawlee.crawlers import FileDownloadCrawler, FileDownloadCrawlingContext

    crawler = FileDownloadCrawler(stream=True)

    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        file_name = URL(context.request.url).name

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
        pipeline = (
            ContextPipeline()
            .compose(self._manage_shared_navigation_timeout)
            .compose(self._execute_pre_navigation_hooks)
            .compose(self._make_http_request)
            .compose(self._execute_post_navigation_hooks)
            .compose(self._handle_status_code_response)
        )

        # In stream mode the request handler consumes the body.
        if not self._stream_response_body:
            pipeline = pipeline.compose(self._read_http_response)

        return pipeline.compose(self._to_file_download_crawling_context)

    async def _to_file_download_crawling_context(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[FileDownloadCrawlingContext, None]:
        """Convert the HTTP crawling context to the final file download crawling context."""
        yield FileDownloadCrawlingContext.from_http_crawling_context(context)
