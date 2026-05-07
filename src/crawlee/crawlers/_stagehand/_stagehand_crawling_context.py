from dataclasses import dataclass

from crawlee._utils.docs import docs_group
from crawlee.browsers import StagehandPage
from crawlee.crawlers import (
    PlaywrightCrawlingContext,
    PlaywrightPostNavCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class StagehandPreNavCrawlingContext(PlaywrightPreNavCrawlingContext):
    """The pre navigation crawling context used by the `StagehandCrawler`."""

    page: StagehandPage


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class StagehandPostNavCrawlingContext(PlaywrightPostNavCrawlingContext, StagehandPreNavCrawlingContext):
    """The post navigation crawling context used by the `StagehandCrawler`."""


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class StagehandCrawlingContext(PlaywrightCrawlingContext, StagehandPostNavCrawlingContext):
    """The crawling context used by the `StagehandCrawler`."""
