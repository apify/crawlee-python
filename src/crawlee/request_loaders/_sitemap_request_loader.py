from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from logging import getLogger
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import override

from crawlee import Request
from crawlee._utils.docs import docs_group
from crawlee._utils.globs import Glob
from crawlee._utils.recoverable_state import RecoverableState
from crawlee._utils.sitemap import NestedSitemap, ParseSitemapOptions, SitemapSource, SitemapUrl, parse_sitemap
from crawlee.request_loaders._request_loader import RequestLoader

if TYPE_CHECKING:
    import re
    from collections.abc import Sequence
    from types import TracebackType

    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.storage_clients.models import ProcessedRequest


logger = getLogger(__name__)


class SitemapRequestLoaderState(BaseModel):
    """State model for persisting sitemap request loader data."""

    model_config = ConfigDict(populate_by_name=True)

    url_queue: Annotated[deque[str], Field(alias='urlQueue')]
    """Queue of URLs extracted from sitemaps and ready for processing."""

    in_progress: Annotated[set[str], Field(alias='inProgress')] = set()
    """Set of request IDs currently being processed."""

    pending_sitemap_urls: Annotated[deque[str], Field(alias='pendingSitemapUrls')]
    """Queue of sitemap URLs that need to be fetched and processed."""

    in_progress_sitemap_url: Annotated[str | None, Field(alias='inProgressSitemapUrl')] = None
    """The sitemap URL currently being processed."""

    current_sitemap_processed_urls: Annotated[set[str], Field(alias='currentSitemapProcessedUrls')] = set()
    """URLs from the current sitemap that have been added to the queue."""

    completed: Annotated[bool, Field(alias='sitemapCompleted')] = False
    """Whether all sitemaps have been fully processed."""

    total_count: Annotated[int, Field(alias='totalCount')] = 0
    """Total number of URLs found and added to the queue from all processed sitemaps."""

    handled_count: Annotated[int, Field(alias='handledCount')] = 0
    """Number of URLs that have been successfully handled."""


@docs_group('Request loaders')
class SitemapRequestLoader(RequestLoader):
    """A request loader that reads URLs from sitemap(s).

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
        """
        self._http_client = http_client
        self._sitemap_urls = sitemap_urls
        self._include = include
        self._exclude = exclude
        self._proxy_info = proxy_info
        self._max_buffer_size = max_buffer_size

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
        if self._state.is_initialized:
            return self._state.current_value

        await self._state.initialize()

        # Initialize pending sitemaps on first run
        if not self._state.current_value.pending_sitemap_urls and not self._state.current_value.completed:
            self._state.current_value.pending_sitemap_urls.extend(self._sitemap_urls)

        if self._state.current_value.url_queue and len(self._state.current_value.url_queue) >= self._max_buffer_size:
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
                        if item.loc not in state.pending_sitemap_urls:
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

                        async with self._queue_lock:
                            state = await self._get_state()
                            state.url_queue.append(url)
                            state.current_sitemap_processed_urls.add(url)
                            state.total_count += 1
                            if len(state.url_queue) >= self._max_buffer_size:
                                # Notify that the queue is full
                                self._queue_has_capacity.clear()

                # Clear current sitemap after processing
                state = await self._get_state()
                state.in_progress_sitemap_url = None
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
        state = await self._get_state()
        if not state.url_queue:
            return None

        async with self._queue_lock:
            url = state.url_queue.popleft()

            request = Request.from_url(url)
            state.in_progress.add(request.id)
            if len(state.url_queue) < self._max_buffer_size:
                self._queue_has_capacity.set()

        return request

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as successfully handled."""
        state = await self._get_state()
        if request.id in state.in_progress:
            state.in_progress.remove(request.id)
            state.handled_count += 1
        return None

    async def abort_loading(self) -> None:
        """Abort the sitemap loading process."""
        if self._loading_task and not self._loading_task.done():
            self._loading_task.cancel()
            await asyncio.sleep(0.01)  # Allow time for cancellation
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
