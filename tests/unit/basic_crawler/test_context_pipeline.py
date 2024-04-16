from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncGenerator
from unittest.mock import AsyncMock

import pytest

from crawlee.basic_crawler.context_pipeline import (
    ContextPipeline,
    ContextPipelineFinalizationError,
    ContextPipelineInitializationError,
    RequestHandlerError,
)
from crawlee.basic_crawler.types import BasicCrawlingContext
from crawlee.request import Request


@dataclass(frozen=True)
class EnhancedCrawlingContext(BasicCrawlingContext):
    foo: str


@dataclass(frozen=True)
class MoreEnhancedCrawlingContext(EnhancedCrawlingContext):
    bar: int


async def test_calls_consumer_without_middleware() -> None:
    consumer = AsyncMock()

    pipeline = ContextPipeline()
    context = BasicCrawlingContext(request=Request.from_url(url='aaa'))

    await pipeline(context, consumer)

    consumer.assert_called_once_with(context)


async def test_calls_consumers_and_middlewares() -> None:
    events = list[str]()

    async def consumer(context: MoreEnhancedCrawlingContext) -> None:
        events.append('consumer_called')
        assert context.bar == 4

    async def middleware_a(context: BasicCrawlingContext) -> AsyncGenerator[EnhancedCrawlingContext, None]:
        events.append('middleware_a_in')
        yield EnhancedCrawlingContext(request=context.request, foo='foo')
        events.append('middleware_a_out')

    async def middleware_b(context: EnhancedCrawlingContext) -> AsyncGenerator[MoreEnhancedCrawlingContext, None]:
        events.append('middleware_b_in')
        yield MoreEnhancedCrawlingContext(request=context.request, foo=context.foo, bar=4)
        events.append('middleware_b_out')

    pipeline = ContextPipeline[BasicCrawlingContext]().compose(middleware_a).compose(middleware_b)

    context = BasicCrawlingContext(request=Request.from_url(url='aaa'))
    await pipeline(context, consumer)

    assert events == [
        'middleware_a_in',
        'middleware_b_in',
        'consumer_called',
        'middleware_b_out',
        'middleware_a_out',
    ]


async def test_wraps_consumer_errors() -> None:
    consumer = AsyncMock(side_effect=RuntimeError('Arbitrary crash for testing purposes'))

    pipeline = ContextPipeline()
    context = BasicCrawlingContext(request=Request.from_url(url='aaa'))

    with pytest.raises(RequestHandlerError):
        await pipeline(context, consumer)


async def test_handles_exceptions_in_middleware_initialization() -> None:
    consumer = AsyncMock()
    cleanup = AsyncMock()

    async def step_1(context: BasicCrawlingContext) -> AsyncGenerator[BasicCrawlingContext, None]:
        yield context
        await cleanup()

    async def step_2(context: BasicCrawlingContext) -> AsyncGenerator[BasicCrawlingContext, None]:
        raise RuntimeError('Crash during middleware initialization')
        yield context

    pipeline = ContextPipeline().compose(step_1).compose(step_2)
    context = BasicCrawlingContext(request=Request.from_url(url='aaa'))

    with pytest.raises(ContextPipelineInitializationError):
        await pipeline(context, consumer)

    assert not consumer.called
    assert cleanup.called


async def test_handles_exceptions_in_middleware_finalization() -> None:
    consumer = AsyncMock()
    cleanup = AsyncMock()

    async def step_1(context: BasicCrawlingContext) -> AsyncGenerator[BasicCrawlingContext, None]:
        yield context
        await cleanup()

    async def step_2(context: BasicCrawlingContext) -> AsyncGenerator[BasicCrawlingContext, None]:
        yield context
        raise RuntimeError('Crash during middleware finalization')

    pipeline = ContextPipeline().compose(step_1).compose(step_2)
    context = BasicCrawlingContext(request=Request.from_url(url='aaa'))

    with pytest.raises(ContextPipelineFinalizationError):
        await pipeline(context, consumer)

    assert consumer.called
    assert not cleanup.called
