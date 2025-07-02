from __future__ import annotations

import asyncio
from contextlib import suppress
from logging import getLogger
from typing import TYPE_CHECKING, Any

from crawlee import Request
from crawlee._utils.docs import docs_group
from crawlee._utils.globs import Glob
from crawlee._utils.sitemap import ParseSitemapOptions, SitemapSource, SitemapUrl, parse_sitemap
from crawlee.request_loaders._request_loader import RequestLoader

if TYPE_CHECKING:
    import re
    from collections.abc import Sequence

    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.storage_clients.models import ProcessedRequest


logger = getLogger(__name__)


@docs_group('Classes')
class SitemapRequestLoader(RequestLoader):
    """A request loader that reads URLs from sitemap(s).

    The loader fetches and parses sitemaps in the background, allowing crawling to start
    before all URLs are loaded. It supports filtering URLs using glob and regex patterns.
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
        parse_sitemap_options: ParseSitemapOptions | None = None,
    ) -> None:
        """Initialize the sitemap request loader.

        Args:
            sitemap_urls: Configuration options for the loader.
            proxy_info: Optional proxy to use for fetching sitemaps.
            include: List of glob or regex patterns to include URLs.
            exclude: List of glob or regex patterns to exclude URLs.
            max_buffer_size: Maximum number of URLs to buffer in memory.
            parse_sitemap_options: Options for parsing sitemaps, such as `SitemapSource` and `max_urls`.
            http_client: the instance of `HttpClient` to use for fetching sitemaps.
        """
        self._http_client = http_client

        self._sitemap_urls = sitemap_urls
        self._include = include
        self._exclude = exclude
        self._proxy_info = proxy_info
        self._parse_sitemap_options = parse_sitemap_options or ParseSitemapOptions()

        self._handled_count = 0
        self._total_count = 0

        # URL queue and tracking
        self._url_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_buffer_size)
        self._in_progress: set[str] = set()
        self._processed_urls: set[str] = set()

        # Loading state
        self._loading_task = asyncio.create_task(self._load_sitemaps())

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
            async for item in parse_sitemap(
                [SitemapSource(type='url', url=url) for url in self._sitemap_urls],
                self._http_client,
                proxy_info=self._proxy_info,
                options=self._parse_sitemap_options,
            ):
                # Only process URL items (not nested sitemaps)
                if isinstance(item, SitemapUrl):
                    url = item.loc

                    # Skip if already processed
                    if url in self._processed_urls:
                        continue

                    # Check if URL should be included
                    if not self._check_url_patterns(url, self._include, self._exclude):
                        continue

                    await self._url_queue.put(url)
                    self._processed_urls.add(url)
                    self._total_count += 1

        except Exception:
            logger.exception('Error loading sitemaps')
            raise

    async def get_total_count(self) -> int:
        """Return the total number of URLs found so far."""
        return self._total_count

    async def is_empty(self) -> bool:
        """Check if there are no more URLs to process."""
        return self._url_queue.empty() and self._loading_task.done()

    async def is_finished(self) -> bool:
        """Check if all URLs have been processed."""
        return self._url_queue.empty() and len(self._in_progress) == 0 and self._loading_task.done()

    async def fetch_next_request(self) -> Request | None:
        """Fetch the next request to process."""
        while not (self._loading_task.done() and self._url_queue.empty()):
            if self._url_queue.empty():
                await asyncio.sleep(0.5)
                continue

            url = await self._url_queue.get()

            request = Request.from_url(url)
            self._in_progress.add(request.id)
            return request

        return None

    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        """Mark a request as successfully handled."""
        if request.id in self._in_progress:
            self._in_progress.remove(request.id)
            self._handled_count += 1
        return None

    async def get_handled_count(self) -> int:
        """Return the number of handled requests."""
        return self._handled_count

    async def abort_loading(self) -> None:
        """Abort the sitemap loading process."""
        if self._loading_task and not self._loading_task.done():
            self._loading_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._loading_task
