from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from logging import getLogger
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import override

from crawlee import Request, RequestOptions
from crawlee._utils.docs import docs_group
from crawlee._utils.globs import Glob
from crawlee._utils.recoverable_state import RecoverableState
from crawlee._utils.sitemap import NestedSitemap, ParseSitemapOptions, SitemapSource, SitemapUrl, parse_sitemap
from crawlee.request_loaders._request_loader import RequestLoader

if TYPE_CHECKING:
    import re
    from collections.abc import Callable, Sequence
    from types import TracebackType

    from crawlee import RequestTransformAction
    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.storage_clients.models import ProcessedRequest


logger = getLogger(__name__)


class SitemapRequestLoaderState(BaseModel):
    """State model for persisting sitemap request loader data.

    The crawler processes one sitemap at a time. The current sitemap is stored in `in_progress_sitemap_url`.
    The `parse_sitemap` function parses the sitemap and returns elements as an async iterator. Each element retrieved
    from the iterator is processed based on its type. If the element is a `NestedSitemap`, its URL is added to
    `pending_sitemap_urls` if it hasn't been processed yet (not in `processed_sitemap_urls`). If the element is a
    `SitemapUrl`, the system checks whether it already exists in `current_sitemap_processed_urls`. If it exists,
    the loader was restarted from a saved state and the URL is skipped.

    If the URL is new, it is first added to `url_queue`, then to `current_sitemap_processed_urls`, and `total_count` is
    incremented by 1. When all elements from the current sitemap iterator have been processed, `in_progress_sitemap_url`
    is set to `None`, the sitemap URL is added to `processed_sitemap_urls`, and `current_sitemap_processed_urls` is
    cleared. The next sitemap is retrieved from `pending_sitemap_urls`, skipping any URLs that already exist in
    `processed_sitemap_urls`. If `pending_sitemap_urls` is empty, `completed` is set to `True`.

    When `fetch_next_request` is called, a URL is extracted from `url_queue` and placed in `in_progress`.
    When `mark_request_as_handled` is called for the extracted URL, it is removed from `in_progress` and
    `handled_count` is incremented by 1.

    During initial startup or restart after persistence, state validation occurs in `_get_state`. If both
    `pending_sitemap_urls` and `in_progress_sitemap_url` are empty and `completed` is False, this indicates a
    fresh start. In this case, `self._sitemap_urls` are moved to `pending_sitemap_urls`. Otherwise, the system is
    restarting from a persisted state. If `in_progress` contains any URLs, they are moved back to `url_queue` and
    `in_progress` is cleared.
    """

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    url_queue: Annotated[deque[str], Field(alias='urlQueue')]
    """Queue of URLs extracted from sitemaps and ready for processing."""

    in_progress: Annotated[set[str], Field(alias='inProgress')] = set()
    """Set of request URLs currently being processed."""

    pending_sitemap_urls: Annotated[deque[str], Field(alias='pendingSitemapUrls')]
    """Queue of sitemap URLs that need to be fetched and processed."""

    in_progress_sitemap_url: Annotated[str | None, Field(alias='inProgressSitemapUrl')] = None
    """The sitemap URL currently being processed."""

    current_sitemap_processed_urls: Annotated[set[str], Field(alias='currentSitemapProcessedUrls')] = set()
    """URLs from the current sitemap that have been added to the queue."""

    processed_sitemap_urls: Annotated[set[str], Field(alias='processedSitemapUrls')] = set()
    """Set of processed sitemap URLs."""

    completed: Annotated[bool, Field(alias='sitemapCompleted')] = False
    """Whether all sitemaps have been fully processed."""

    total_count: Annotated[int, Field(alias='totalCount')] = 0
    """Total number of URLs found and added to the queue from all processed sitemaps."""

    handled_count: Annotated[int, Field(alias='handledCount')] = 0
    """Number of URLs that have been successfully handled."""


@docs_group('Request loaders')
class SitemapRequestLoader(RequestLoader):
    """A request loader that reads URLs from sitemap(s).

    The loader is designed to handle sitemaps that follow the format described in the Sitemaps protocol
    (https://www.sitemaps.org/protocol.html). It supports both XML and plain text sitemap formats.
    Note that HTML pages containing links are not supported - those should be handled by regular crawlers
    and the `enqueue_links` functionality.

    The loader fetches and parses sitemaps in the background, allowing crawling to start
    before all URLs are loaded. It supports filtering URLs using glob and regex patterns.

    The loader supports state persistence, allowing it to resume from where it left off
    after interruption when a `persist_state_key` is provided during initialization.
    """

    def __init__(
        self,
        sitemap_urls: list[str],
        http_client: HttpClient,
        *,
        proxy_info: ProxyInfo | None = None,
        include: list[re.Pattern[Any] | Glob] | None = None,
        exclude: list[re.Pattern[Any] | Glob] | None = None,
        max_buffer_size: int = 200,
        persist_state_key: str | None = None,
        transform_request_function: Callable[[RequestOptions], RequestOptions | RequestTransformAction] | None = None,
    ) -> None:
        """Initialize the sitemap request loader.

        Args:
            sitemap_urls: Configuration options for the loader.
            proxy_info: Optional proxy to use for fetching sitemaps.
            include: List of glob or regex patterns to include URLs.
            exclude: List of glob or regex patterns to exclude URLs.
            max_buffer_size: Maximum number of URLs to buffer in memory.
            http_client: the instance of `HttpClient` to use for fetching sitemaps.
            persist_state_key: A key for persisting the loader's state in the KeyValueStore.
                When provided, allows resuming from where it left off after interruption.
                If None, no state persistence occurs.
            transform_request_function: An optional function to transform requests
                generated by the loader. It receives `RequestOptions` with `url` and should return either
                modified `RequestOptions` or a `RequestTransformAction`.
        """
        self._http_client = http_client
        self._sitemap_urls = sitemap_urls
        self._include = include
        self._exclude = exclude
        self._proxy_info = proxy_info
        self._max_buffer_size = max_buffer_size
        self._transform_request_function = transform_request_function

        # Synchronization for queue operations
        self._queue_has_capacity = asyncio.Event()
        self._queue_has_capacity.set()
        self._queue_lock = asyncio.Lock()

        # Initialize recoverable state
        self._state = RecoverableState(
            default_state=SitemapRequestLoaderState(
                url_queue=deque(),
                pending_sitemap_urls=deque(),
            ),
            persistence_enabled=bool(persist_state_key),
            persist_state_key=persist_state_key or '',
            logger=logger,
        )

        # Start background loading
        self._loading_task = asyncio.create_task(self._load_sitemaps())

    async def _get_state(self) -> SitemapRequestLoaderState:
        """Initialize and return the current state."""
        async with self._queue_lock:
            if self._state.is_initialized:
                return self._state.current_value

            await self._state.initialize()

            # Initialize pending sitemaps on first run
            has_sitemap_for_processing = (
                self._state.current_value.pending_sitemap_urls or self._state.current_value.in_progress_sitemap_url
            )
            if not has_sitemap_for_processing and not self._state.current_value.completed:
                self._state.current_value.pending_sitemap_urls.extend(self._sitemap_urls)

            if self._state.current_value.in_progress:
                self._state.current_value.url_queue.extendleft(self._state.current_value.in_progress)
                self._state.current_value.in_progress.clear()

            if (
                self._state.current_value.url_queue
                and len(self._state.current_value.url_queue) >= self._max_buffer_size
            ):
                # Notify that the queue is full
                self._queue_has_capacity.clear()

            return self._state.current_value

    def _check_url_patterns(
        self,
        target_url: str,
        include: Sequence[re.Pattern[Any] | Glob] | None,
        exclude: Sequence[re.Pattern[Any] | Glob] | None,
    ) -> bool:
        """Check if a URL matches configured include/exclude patterns."""
        # If the URL matches any `exclude` pattern, reject it
        for pattern in exclude or ():
            if isinstance(pattern, Glob):
                pattern = pattern.regexp  # noqa: PLW2901

            if pattern.match(target_url) is not None:
                return False

        # If there are no `include` patterns and the URL passed all `exclude` patterns, accept the URL
        if include is None:
            return True

        # If the URL matches any `include` pattern, accept it
        for pattern in include:
            if isinstance(pattern, Glob):
                pattern = pattern.regexp  # noqa: PLW2901

            if pattern.match(target_url) is not None:
                return True

        # The URL does not match any `include` pattern - reject it
        return False

    async def _load_sitemaps(self) -> None:
        """Load URLs from sitemaps in the background."""
        try:
            # Get actual state
            while (state := await self._get_state()) and (state.pending_sitemap_urls or state.in_progress_sitemap_url):
                # Get sitemap URL for parsing
                sitemap_url = state.in_progress_sitemap_url
                if not sitemap_url:
                    sitemap_url = state.pending_sitemap_urls.popleft()
                    # Skip processed urls
                    if sitemap_url in state.processed_sitemap_urls:
                        continue
                    state.in_progress_sitemap_url = sitemap_url

                parse_options = ParseSitemapOptions(max_depth=0, emit_nested_sitemaps=True)

                async for item in parse_sitemap(
                    [SitemapSource(type='url', url=sitemap_url)],
                    self._http_client,
                    proxy_info=self._proxy_info,
                    options=parse_options,
                ):
                    if isinstance(item, NestedSitemap):
                        # Add nested sitemap to queue
                        if item.loc not in state.pending_sitemap_urls and item.loc not in state.processed_sitemap_urls:
                            state.pending_sitemap_urls.append(item.loc)
                        continue

                    if isinstance(item, SitemapUrl):
                        url = item.loc

                        state = await self._get_state()

                        # Skip if already processed
                        if url in state.current_sitemap_processed_urls:
                            continue

                        # Check if URL should be included
                        if not self._check_url_patterns(url, self._include, self._exclude):
                            continue

                        # Check if we have capacity in the queue
                        await self._queue_has_capacity.wait()

                        state = await self._get_state()
                        async with self._queue_lock:
                            state.url_queue.append(url)
                            state.current_sitemap_processed_urls.add(url)
                            state.total_count += 1
                            if len(state.url_queue) >= self._max_buffer_size:
                                # Notify that the queue is full
                                self._queue_has_capacity.clear()

                # Clear current sitemap after processing
                state = await self._get_state()
                current_sitemap_url = state.in_progress_sitemap_url
                state.in_progress_sitemap_url = None
                if current_sitemap_url:
                    state.processed_sitemap_urls.add(current_sitemap_url)
                state.current_sitemap_processed_urls.clear()

            # Mark as completed after processing all sitemap urls
            state.completed = True

        except Exception:
            logger.exception('Error loading sitemaps')
            raise

    @override
    async def get_total_count(self) -> int:
        """Return the total number of URLs found so far."""
        state = await self._get_state()
        return state.total_count

    @override
    async def get_handled_count(self) -> int:
        """Return the number of URLs that have been handled."""
        state = await self._get_state()
        return state.handled_count

    @override
    async def is_empty(self) -> bool:
        """Check if there are no more URLs to process."""
        state = await self._get_state()
        return not state.url_queue

    @override
    async def is_finished(self) -> bool:
        """Check if all URLs have been processed."""
        state = await self._get_state()
        return not state.url_queue and len(state.in_progress) == 0 and self._loading_task.done()

    @override
    async def fetch_next_request(self) -> Request | None:
        """Fetch the next request to process."""
        while not (await self.is_finished()):
            state = await self._get_state()
            if not state.url_queue:
                await asyncio.sleep(0.1)
                continue

            async with self._queue_lock:
                url = state.url_queue.popleft()
                request_option = RequestOptions(url=url)
                if self._transform_request_function:
                    transform_request_option = self._transform_request_function(request_option)
                    if transform_request_option == 'skip':
                        state.total_count -= 1
                        continue
                    if transform_request_option != 'unchanged':
                        request_option = transform_request_option
                request = Request.from_url(**request_option)
                state.in_progress.add(request.url)
                if len(state.url_queue) < self._max_buffer_size:
                    self._queue_has_capacity.set()

            return request

        return None

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as successfully handled."""
        state = await self._get_state()
        if request.url in state.in_progress:
            state.in_progress.remove(request.url)
            state.handled_count += 1
        return None

    async def abort_loading(self) -> None:
        """Abort the sitemap loading process."""
        if self._loading_task and not self._loading_task.done():
            self._loading_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._loading_task

    async def start(self) -> None:
        """Start the sitemap loading process."""
        if self._loading_task and not self._loading_task.done():
            return
        self._loading_task = asyncio.create_task(self._load_sitemaps())

    async def close(self) -> None:
        """Close the request loader."""
        await self.abort_loading()
        await self._state.teardown()

    async def __aenter__(self) -> SitemapRequestLoader:
        """Enter the context manager."""
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, exc_traceback: TracebackType | None
    ) -> None:
        """Exit the context manager."""
        await self.close()
