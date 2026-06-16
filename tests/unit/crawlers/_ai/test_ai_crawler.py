from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest
from parsel import Selector
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from crawlee import Request
from crawlee.crawlers import AiCrawler, AiCrawlingContext, AiDirectExtractor, ParselCrawlingContext
from crawlee.crawlers._ai._types import AiUsageStats

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.http_clients._base import HttpClient


class _Article(BaseModel):
    title: str


def test_requires_exactly_one_of_model_or_extractor() -> None:
    with pytest.raises(ValueError, match='exactly one'):
        AiCrawler()

    with pytest.raises(ValueError, match='exactly one'):
        AiCrawler(model=TestModel(), extractor=AiDirectExtractor(TestModel()))


def test_default_extractor_is_direct() -> None:
    assert isinstance(AiCrawler(model=TestModel()).extractor, AiDirectExtractor)


def test_emits_experimental_warning() -> None:
    with pytest.warns(UserWarning, match='experimental'):
        AiCrawler(model=TestModel())


def test_exposes_extractor_and_usage() -> None:
    extractor = AiDirectExtractor(TestModel())
    crawler = AiCrawler(extractor=extractor)

    assert crawler.extractor is extractor
    assert crawler.ai_usage is extractor.ai_usage


async def test_context_extract(server_url: URL, http_client: HttpClient) -> None:
    crawler = AiCrawler(model=TestModel(custom_output_args={'title': 'Hello'}), http_client=http_client)
    extracted = AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: AiCrawlingContext) -> None:
        await extracted(await context.extract(_Article))

    await crawler.run([str(server_url / 'start_enqueue')])

    extracted.assert_awaited_once_with(_Article(title='Hello'))
    assert crawler.ai_usage.requests == 1


async def test_crawling_context_type(server_url: URL, http_client: HttpClient) -> None:
    crawler = AiCrawler(model=TestModel(custom_output_args={'title': 'Hello'}), http_client=http_client)
    handler = AsyncMock()
    crawler.router.default_handler(handler)

    await crawler.run([str(server_url / 'start_enqueue')])

    handler.assert_awaited_once()
    context = handler.call_args.args[0]

    # It extends the Parsel context, so the manual `selector` stays available next to the AI helpers.
    assert isinstance(context, AiCrawlingContext)
    assert isinstance(context, ParselCrawlingContext)
    assert isinstance(context.selector, Selector)
    assert isinstance(context.ai_usage, AiUsageStats)


async def test_context_extractor_forwards_arguments(server_url: URL, http_client: HttpClient) -> None:
    extractor = AiDirectExtractor(TestModel())
    crawler = AiCrawler(extractor=extractor, http_client=http_client)
    extract_mock = AsyncMock(return_value=_Article(title='test'))
    seen_selector = AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: AiCrawlingContext) -> None:
        await seen_selector(context.selector)
        await context.extract(_Article)  # cache_tag defaults to the request label
        await context.extract(_Article, cache_tag='explicit')  # an explicit tag overrides the default
        await context.extract(_Article, scope='article', additional_instructions='hint')

    with patch.object(extractor, 'extract', extract_mock):
        await crawler.run([Request.from_url(str(server_url), label='detail')])

    first_call, second_call, third_call = extract_mock.call_args_list

    # The live parsed selector is handed over as the first positional argument, without a re-parse.
    assert first_call.args[0] is seen_selector.call_args.args[0]
    assert first_call.kwargs['cache_tag'] == 'detail'
    assert second_call.kwargs['cache_tag'] == 'explicit'
    assert third_call.kwargs['scope'] == 'article'
    assert third_call.kwargs['additional_instructions'] == 'hint'


def test_import_error_handled() -> None:
    # The `ai` extra is optional, so accessing the crawler without `pydantic_ai` installed must raise a clear error.
    blocked = {name: None for name in sys.modules if name == 'pydantic_ai' or name.startswith('pydantic_ai.')}
    with patch.dict('sys.modules', blocked):
        for name in list(sys.modules):
            if name.startswith('crawlee.crawlers._ai'):
                sys.modules.pop(name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers._ai import AiCrawler  # noqa: F401 PLC0415
