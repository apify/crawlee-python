from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
from typing import TYPE_CHECKING, Literal, TypedDict
from xml.sax.handler import ContentHandler

from typing_extensions import override
from yarl import URL

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from xml.sax.xmlreader import AttributesImpl

logger = getLogger(__name__)


@dataclass()
class SitemapUrl:
    loc: str
    lastmod: datetime | None
    changefreq: Literal['always', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'never'] | None
    priority: float | None
    origin_sitemap_url: str | None


@dataclass()
class NestedSitemap:
    loc: str
    origin_sitemap_url: str | None


class ParseSitemapOptions(TypedDict, total=False):
    emit_nested_sitemaps: bool
    max_depth: int
    sitemap_retries: int
    timeout: float | None


class SitemapSource(TypedDict, total=False):
    type: Literal['url', 'raw']
    url: str
    content: str
    depth: int


class SitemapItem(TypedDict, total=False):
    type: Literal['url', 'sitemap_url']
    loc: str
    url: str
    lastmod: datetime
    changefreq: str
    priority: float


class SitemapHandler(ContentHandler):
    def __init__(self) -> None:
        super().__init__()
        self.root_tag_name: str | None = None
        self.current_tag: str | None = None
        self.current_url: SitemapItem = {}
        self.buffer: str = ''
        self.items: list[SitemapItem] = []

    @override
    def startElement(self, name: str, attrs: AttributesImpl) -> None:
        if self.root_tag_name is None and name in ('urlset', 'sitemapindex'):
            self.root_tag_name = name

        if name in ('loc', 'lastmod', 'changefreq', 'priority'):
            self.current_tag = name
            self.buffer = ''

    def characters(self, content: str) -> None:
        if self.current_tag:
            self.buffer += content

    @override
    def endElement(self, name: str) -> None:
        changefreq_atr = ('always', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'never')
        if name == self.current_tag:
            text = self.buffer.strip()

            if name == 'loc':
                if self.root_tag_name == 'sitemapindex':
                    self.items.append({'type': 'sitemap_url', 'url': text})
                else:
                    self.current_url['loc'] = text

            elif name == 'lastmod' and text:
                with suppress(ValueError):
                    self.current_url['lastmod'] = datetime.fromisoformat(text.replace('Z', '+00:00'))

            elif name == 'priority' and text:
                with suppress(ValueError):
                    self.current_url['priority'] = float(text)

            elif name == 'changefreq' and (text and text in changefreq_atr):
                self.current_url['changefreq'] = text

            self.current_tag = None

        if name == 'url' and 'loc' in self.current_url:
            self.items.append({'type': 'url', **self.current_url})
            self.current_url = {}


class Sitemap:
    def __init__(self, urls: list[str]) -> None:
        self.urls = urls

    @classmethod
    async def try_common_names(cls, url: str, proxy_url: str | None = None) -> Sitemap:
        base_url = URL(url)

        sitemap_urls = [str(base_url.with_path('/sitemap.xml')), str(base_url.with_path('/sitemap.txt'))]

        return await cls.load(sitemap_urls, proxy_url)

    @classmethod
    async def load(
        cls,
        urls: str | list[str],
        proxy_url: str | None = None,
        parse_sitemap_options: ParseSitemapOptions | None = None,
    ) -> Sitemap:
        if isinstance(urls, str):
            urls = [urls]

        return await cls.parse(
            [{'type': 'url', 'url': url} for url in urls],
            proxy_url,
            parse_sitemap_options,
        )

    @classmethod
    async def from_xml_string(cls, content: str, proxy_url: str | None = None) -> Sitemap:
        return await cls.parse([{'type': 'raw', 'content': content}], proxy_url)

    @classmethod
    async def parse(
        cls,
        sources: list[SitemapSource],
        proxy_url: str | None = None,
        parse_sitemap_options: ParseSitemapOptions | None = None,
    ) -> Sitemap:
        urls: list[str] = []

        urls = [item.loc async for item in parse_sitemap(sources, proxy_url, parse_sitemap_options)]

        return cls(urls)


# mypy: ignore-errors
async def parse_sitemap(
    sources: list[SitemapSource],
    proxy_url: str | None = None,
    parse_sitemap_options: ParseSitemapOptions | None = None,
) -> AsyncGenerator[SitemapUrl | NestedSitemap, None]:
    raise NotImplementedError('This is a stub')
