from __future__ import annotations

import asyncio
import zlib
from codecs import getincrementaldecoder
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from logging import getLogger
from typing import TYPE_CHECKING, Literal, TypedDict
from xml.sax.expatreader import ExpatParser
from xml.sax.handler import ContentHandler

from typing_extensions import NotRequired, override
from yarl import URL

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from xml.sax.xmlreader import AttributesImpl

    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo

logger = getLogger(__name__)

VALID_CHANGE_FREQS = {'always', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'never'}
SITEMAP_HEADERS = {'accept': 'text/plain, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8'}


@dataclass()
class SitemapUrl:
    loc: str
    lastmod: datetime | None = None
    changefreq: str | None = None
    priority: float | None = None
    origin_sitemap_url: str | None = None


@dataclass()
class NestedSitemap:
    loc: str
    origin_sitemap_url: str | None = None


class ParseSitemapOptions(TypedDict, total=False):
    emit_nested_sitemaps: bool
    max_depth: int
    sitemap_retries: int
    timeout: timedelta | None


class SitemapSource(TypedDict):
    type: Literal['url', 'raw']
    url: NotRequired[str]
    content: NotRequired[str]
    depth: NotRequired[int]


class _SitemapItem(TypedDict, total=False):
    type: Literal['url', 'sitemap_url']
    loc: str
    url: str
    lastmod: datetime | None
    changefreq: str | None
    priority: float | None


class _XMLSaxSitemapHandler(ContentHandler):
    def __init__(self) -> None:
        super().__init__()
        self._root_tag_name: str | None = None
        self._current_tag: str | None = None
        self._current_url: _SitemapItem = {}
        self._buffer: str = ''
        self._items: list[_SitemapItem] = []

    @property
    def items(self) -> list[_SitemapItem]:
        return self._items

    @override
    def startElement(self, name: str, attrs: AttributesImpl) -> None:
        if self._root_tag_name is None and name in ('urlset', 'sitemapindex'):
            self._root_tag_name = name

        if name in ('loc', 'lastmod', 'changefreq', 'priority'):
            self._current_tag = name
            self._buffer = ''

    def characters(self, content: str) -> None:
        if self._current_tag:
            self._buffer += content

    @override
    def endElement(self, name: str) -> None:
        if name == self._current_tag:
            text = self._buffer.strip()

            if name == 'loc':
                if self._root_tag_name == 'sitemapindex':
                    self._items.append({'type': 'sitemap_url', 'url': text})
                else:
                    self._current_url['loc'] = text

            elif name == 'lastmod' and text:
                with suppress(ValueError):
                    self._current_url['lastmod'] = datetime.fromisoformat(text.replace('Z', '+00:00'))

            elif name == 'priority' and text:
                with suppress(ValueError):
                    self._current_url['priority'] = float(text)

            elif name == 'changefreq' and text in VALID_CHANGE_FREQS:
                self._current_url['changefreq'] = text

            self.current_tag = None

        if name == 'url' and 'loc' in self._current_url:
            self.items.append({'type': 'url', **self._current_url})
            self._current_url = {}


class _TxtSitemapParser:
    """Parser for plaintext sitemaps that processes data as a stream."""

    def __init__(self) -> None:
        self._buffer = ''

    async def process_chunk(self, chunk: str) -> AsyncGenerator[_SitemapItem, None]:
        """Process a chunk of text data and yield items one by one."""
        self._buffer += chunk

        # Process complete lines
        if '\n' in self._buffer:
            lines = self._buffer.split('\n')
            # Last element might be incomplete, save for next chunk
            self._buffer = lines.pop()

            for line in lines:
                url = line.strip()
                if url:
                    yield {'type': 'url', 'loc': url}

    async def flush(self) -> AsyncGenerator[_SitemapItem, None]:
        """Process any remaining data in the buffer, yielding items one by one."""
        if self._buffer:
            url = self._buffer.strip()
            if url:
                yield {'type': 'url', 'loc': url}
            self.buffer = ''

    def close(self) -> None:
        """Clean up resources."""
        self._buffer = ''


class _XmlSitemapParser:
    """Parser for XML sitemaps using SAX to process data as a stream."""

    def __init__(self) -> None:
        self._parser = ExpatParser()
        self._handler = _XMLSaxSitemapHandler()
        self._parser.setContentHandler(self._handler)

    async def process_chunk(self, chunk: str) -> AsyncGenerator[_SitemapItem, None]:
        """Process a chunk of XML data and yield items one by one."""
        try:
            self._parser.feed(chunk)

            # If we get here, the XML was valid and complete
            for item in self._handler.items:
                yield item

            self._handler.items.clear()

        except Exception as e:
            logger.warning(f'Failed to parse XML data chunk: {e}', exc_info=True)

    async def flush(self) -> AsyncGenerator[_SitemapItem, None]:
        """Process any remaining data in the buffer, yielding items one by one."""
        try:
            self._parser.flush()

            for item in self._handler.items:
                yield item

            self._handler.items.clear()

        except Exception as e:
            logger.warning(f'Failed to parse remaining XML data: {e}')

    def close(self) -> None:
        """Clean up resources."""
        self._parser.close()


def _get_parser(content_type: str = '', url: str | None = None) -> _XmlSitemapParser | _TxtSitemapParser:
    """Create appropriate parser based on content type and URL."""
    if 'text/plain' in content_type.lower() or (url and URL(url).path.endswith('.txt')):
        return _TxtSitemapParser()
    # Default to XML parser for most cases
    return _XmlSitemapParser()


def _get_origin_url(source: SitemapSource) -> str:
    """Determine the origin URL for a sitemap source."""
    if source['type'] == 'url' and 'url' in source:
        return source['url']
    if source['type'] == 'raw' and 'content' in source:
        # For raw content sources, create a consistent identifier
        return f'raw://{sha256(source["content"].encode()).hexdigest()}'
    return ''


async def _process_sitemap_item(
    item: _SitemapItem,
    source: SitemapSource,
    depth: int,
    visited_sitemap_urls: set[str],
    sources: list[SitemapSource],
    *,
    emit_nested_sitemaps: bool,
) -> AsyncGenerator[SitemapUrl | NestedSitemap | None, None]:
    """Process a sitemap item and yield appropriate results."""
    item_copy = item.copy()  # Work with a copy to avoid modifying the original

    if 'type' not in item_copy:
        return

    item_type = item_copy.pop('type')

    # Handle sitemap URL references (nested sitemaps)
    if item_type == 'sitemap_url' and 'url' in item_copy:
        sitemap_url = item_copy['url']
        if sitemap_url and sitemap_url not in visited_sitemap_urls:
            # Add to processing queue
            sources.append(SitemapSource(type='url', url=sitemap_url, depth=depth + 1))

            # Output the nested sitemap reference if requested
            if emit_nested_sitemaps:
                yield NestedSitemap(loc=sitemap_url, origin_sitemap_url=None)

    # Handle individual URL entries
    elif item_type == 'url' and 'loc' in item_copy:
        # Determine the origin sitemap URL for tracking purposes
        origin_url = _get_origin_url(source)

        # Create and yield the sitemap URL object
        yield SitemapUrl(
            loc=item_copy['loc'],
            lastmod=item_copy.get('lastmod'),
            changefreq=item_copy.get('changefreq'),
            priority=item_copy.get('priority'),
            origin_sitemap_url=origin_url,
        )


async def _process_raw_source(
    source: SitemapSource,
    depth: int,
    visited_sitemap_urls: set[str],
    sources: list[SitemapSource],
    *,
    emit_nested_sitemaps: bool,
) -> AsyncGenerator[SitemapUrl | NestedSitemap, None]:
    """Process a raw content sitemap source."""
    if 'content' not in source:
        logger.warning(f'Raw source missing content: {source}')
        return

    content = source['content']
    parser = _get_parser('text/xml')

    try:
        # Process the content
        async for item in parser.process_chunk(content):
            async for result in _process_sitemap_item(
                item, source, depth, visited_sitemap_urls, sources, emit_nested_sitemaps=emit_nested_sitemaps
            ):
                if result:
                    yield result

        # Process any remaining content
        async for item in parser.flush():
            async for result in _process_sitemap_item(
                item, source, depth, visited_sitemap_urls, sources, emit_nested_sitemaps=emit_nested_sitemaps
            ):
                if result:
                    yield result
    except Exception as e:
        logger.warning(f'Failed to parse raw sitemap content: {e}')
    finally:
        parser.close()


async def _fetch_and_process_sitemap(
    http_client: HttpClient,
    source: SitemapSource,
    depth: int,
    visited_sitemap_urls: set[str],
    sources: list[SitemapSource],
    retries_left: int,
    *,
    proxy_info: ProxyInfo | None = None,
    timeout: timedelta | None = None,
    emit_nested_sitemaps: bool,
) -> AsyncGenerator[SitemapUrl | NestedSitemap, None]:
    """Fetch a sitemap from a URL and process its content."""
    if 'url' not in source:
        return

    sitemap_url = source['url']

    try:
        while retries_left > 0:
            retries_left -= 1
            async with http_client.stream(
                sitemap_url, method='GET', headers=SITEMAP_HEADERS, proxy_info=proxy_info, timeout=timeout
            ) as response:
                # Determine content type and compression
                content_type = response.headers.get('content-type', '')

                decoder = getincrementaldecoder('utf-8')(errors='replace')

                # Create appropriate parser
                parser = _get_parser(content_type, sitemap_url)
                decompressor = None
                try:
                    # Process chunks as they arrive
                    first_chunk = True
                    async for raw_chunk in response.read_stream():
                        # Check if the first chunk is a valid gzip header
                        if first_chunk and raw_chunk.startswith(b'\x1f\x8b'):
                            decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                            first_chunk = False

                        chunk = decompressor.decompress(raw_chunk) if decompressor else raw_chunk
                        text_chunk = decoder.decode(chunk)
                        async for item in parser.process_chunk(text_chunk):
                            async for result in _process_sitemap_item(
                                item,
                                source,
                                depth,
                                visited_sitemap_urls,
                                sources,
                                emit_nested_sitemaps=emit_nested_sitemaps,
                            ):
                                if result:
                                    yield result

                    # Process any remaining content
                    async for item in parser.flush():
                        async for result in _process_sitemap_item(
                            item,
                            source,
                            depth,
                            visited_sitemap_urls,
                            sources,
                            emit_nested_sitemaps=emit_nested_sitemaps,
                        ):
                            if result:
                                yield result
                finally:
                    parser.close()
                break

    except Exception as e:
        if retries_left > 0:
            logger.warning(f'Error fetching sitemap {sitemap_url}: {e}. Retries left: {retries_left}')
            await asyncio.sleep(1)  # Brief pause before retry


class Sitemap:
    def __init__(self, urls: list[str]) -> None:
        self._urls = urls

    @property
    def urls(self) -> list[str]:
        return self._urls

    @classmethod
    async def try_common_names(cls, url: str, http_client: HttpClient, proxy_info: ProxyInfo | None = None) -> Sitemap:
        base_url = URL(url)
        sitemap_urls = [str(base_url.with_path('/sitemap.xml')), str(base_url.with_path('/sitemap.txt'))]
        return await cls.load(sitemap_urls, http_client, proxy_info)

    @classmethod
    async def load(
        cls,
        urls: str | list[str],
        http_client: HttpClient,
        proxy_info: ProxyInfo | None = None,
        parse_sitemap_options: ParseSitemapOptions | None = None,
    ) -> Sitemap:
        if isinstance(urls, str):
            urls = [urls]
        return await cls.parse(
            [SitemapSource(type='url', url=url) for url in urls], http_client, proxy_info, parse_sitemap_options
        )

    @classmethod
    async def from_xml_string(cls, content: str) -> Sitemap:
        return await cls.parse([SitemapSource(type='raw', content=content)])

    @classmethod
    async def parse(
        cls,
        sources: list[SitemapSource],
        http_client: HttpClient | None = None,
        proxy_info: ProxyInfo | None = None,
        parse_sitemap_options: ParseSitemapOptions | None = None,
    ) -> Sitemap:
        urls = [item.loc async for item in parse_sitemap(sources, http_client, proxy_info, parse_sitemap_options)]
        return cls(urls)


async def parse_sitemap(
    initial_sources: list[SitemapSource],
    http_client: HttpClient | None = None,
    proxy_info: ProxyInfo | None = None,
    options: ParseSitemapOptions | None = None,
) -> AsyncGenerator[SitemapUrl | NestedSitemap, None]:
    """Parse sitemap(s) and yield URLs found in them.

    This function coordinates the process of fetching and parsing sitemaps,
    handling both URL-based and raw content sources. It follows nested sitemaps
    up to the specified maximum depth.
    """
    # Set default options
    options = options or {}
    emit_nested_sitemaps = options.get('emit_nested_sitemaps', False)
    max_depth = options.get('max_depth', float('inf'))
    sitemap_retries = options.get('sitemap_retries', 3)

    # Setup working state
    sources = list(initial_sources)
    visited_sitemap_urls: set[str] = set()

    # Process sources until the queue is empty
    while sources:
        source = sources.pop(0)
        depth = source.get('depth', 0)

        # Skip if we've reached max depth
        if depth > max_depth:
            logger.debug(f'Skipping sitemap {source.get("url", "")} - exceeded max depth {max_depth}')
            continue

        # Process based on source type
        if source['type'] == 'raw':
            async for result in _process_raw_source(
                source, depth, visited_sitemap_urls, sources, emit_nested_sitemaps=emit_nested_sitemaps
            ):
                yield result

        elif source['type'] == 'url' and 'url' in source:
            # Add to visited set before processing to avoid duplicates
            if http_client is None:
                raise RuntimeError('HttpClient must be provided for URL-based sitemap sources.')

            visited_sitemap_urls.add(source['url'])

            async for result in _fetch_and_process_sitemap(
                http_client,
                source,
                depth,
                visited_sitemap_urls,
                sources,
                sitemap_retries,
                emit_nested_sitemaps=emit_nested_sitemaps,
                proxy_info=proxy_info,
                timeout=options.get('timeout', timedelta(seconds=30)),
            ):
                yield result
        else:
            logger.warning(f'Invalid source configuration: {source}')
