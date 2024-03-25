from __future__ import annotations

from typing import Awaitable, Callable, Generic, cast

from typing_extensions import TypeVar

from crawlee.basic_crawler.types import BasicCrawlingContext

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TMiddlewareCrawlingContext = TypeVar('TMiddlewareCrawlingContext', bound=BasicCrawlingContext)
MiddlewareCallNext = Callable[[TMiddlewareCrawlingContext], Awaitable[None]]


class RequestHandlerError(Exception, Generic[TCrawlingContext]):
    """Wraps an exception thrown from a request handler (router) and extends it with crawling context."""

    def __init__(self, wrapped_exception: Exception, crawling_context: TCrawlingContext) -> None:
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


class ContextPipeline(Generic[TCrawlingContext]):
    def __init__(
        self,
        *,
        _middleware: Callable[[BasicCrawlingContext, MiddlewareCallNext[BasicCrawlingContext]], Awaitable[None]]
        | None = None,
        _parent: ContextPipeline[BasicCrawlingContext] | None = None,
    ) -> None:
        self._middleware = _middleware
        self._parent = _parent

    async def __call__(
        self,
        crawling_context: BasicCrawlingContext,
        final_context_consumer: Callable[[TCrawlingContext], Awaitable[None]],
    ) -> None:
        if not self._middleware:
            return

        async def call_next(
            enhanced_context: BasicCrawlingContext,
        ) -> None:
            if self._parent:
                await self._parent(
                    enhanced_context, cast(Callable[[BasicCrawlingContext], Awaitable[None]], final_context_consumer)
                )
            else:
                try:
                    await final_context_consumer(cast(TCrawlingContext, enhanced_context))
                except Exception as e:
                    raise RequestHandlerError(e, cast(TCrawlingContext, enhanced_context)) from e

        await self._middleware(crawling_context, call_next)

    def compose(
        self, middleware: Callable[[TCrawlingContext, MiddlewareCallNext[TMiddlewareCrawlingContext]], Awaitable[None]]
    ) -> ContextPipeline[TMiddlewareCrawlingContext]:
        return ContextPipeline[TMiddlewareCrawlingContext](
            _middleware=cast(
                Callable[[BasicCrawlingContext, MiddlewareCallNext[BasicCrawlingContext]], Awaitable[None]], middleware
            ),
            _parent=cast(ContextPipeline[BasicCrawlingContext], self),
        )
