from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

from parsel import Selector
from pydantic import BaseModel
from pydantic_ai.models.test import TestModel

from crawlee import Request
from crawlee.crawlers import (
    ParselCrawlingContext,
    PydanticAiCrawler,
    PydanticAiCrawlingContext,
    PydanticAiDirectExtractor,
)
from crawlee.crawlers._pydantic_ai._types import PydanticAiUsageStats

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.http_clients._base import HttpClient


class _Article(BaseModel):
    title: str


async def test_context_extract(server_url: URL, http_client: HttpClient) -> None:
    crawler = PydanticAiCrawler(model=TestModel(custom_output_args={'title': 'Hello'}), http_client=http_client)
    extracted = AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: PydanticAiCrawlingContext) -> None:
        await extracted(await context.extract(_Article))

    await crawler.run([str(server_url / 'start_enqueue')])

    extracted.assert_awaited_once_with(_Article(title='Hello'))
    assert crawler.ai_usage.requests == 1


async def test_crawling_context_type(server_url: URL, http_client: HttpClient) -> None:
    crawler = PydanticAiCrawler(model=TestModel(custom_output_args={'title': 'Hello'}), http_client=http_client)
    handler = AsyncMock()
    crawler.router.default_handler(handler)

    await crawler.run([str(server_url / 'start_enqueue')])

    handler.assert_awaited_once()
    context = handler.call_args.args[0]

    # It extends the Parsel context, so the manual `selector` stays available next to the AI helpers.
    assert isinstance(context, PydanticAiCrawlingContext)
    assert isinstance(context, ParselCrawlingContext)
    assert isinstance(context.selector, Selector)
    assert isinstance(context.ai_usage, PydanticAiUsageStats)


async def test_context_extractor_forwards_arguments(server_url: URL, http_client: HttpClient) -> None:
    extractor = PydanticAiDirectExtractor(TestModel())
    crawler = PydanticAiCrawler(extractor=extractor, http_client=http_client)
    extract_mock = AsyncMock(return_value=_Article(title='test'))
    seen_selector = AsyncMock()

    @crawler.router.default_handler
    async def request_handler(context: PydanticAiCrawlingContext) -> None:
        await seen_selector(context.selector)
        await context.extract(_Article)  # cache_tag defaults to the request label
        await context.extract(_Article, cache_tag='explicit')  # an explicit tag overrides the default
        await context.extract(_Article, scope='article', additional_instructions='hint')

    with patch.object(extractor, 'extract', extract_mock):
        async with extractor:
            await crawler.run([Request.from_url(str(server_url), label='detail')])

    first_call, second_call, third_call = extract_mock.call_args_list

    # The live parsed selector is handed over as the first positional argument, without a re-parse.
    assert first_call.args[0] is seen_selector.call_args.args[0]
    assert first_call.kwargs['cache_tag'] == 'detail'
    assert second_call.kwargs['cache_tag'] == 'explicit'
    assert third_call.kwargs['scope'] == 'article'
    assert third_call.kwargs['additional_instructions'] == 'hint'
