from __future__ import annotations

from typing import Any, AsyncGenerator, Awaitable, Callable, Generator, Generic, cast

from typing_extensions import TypeVar

from crawlee.basic_crawler.types import BasicCrawlingContext

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TMiddlewareCrawlingContext = TypeVar('TMiddlewareCrawlingContext', bound=BasicCrawlingContext)


class RequestHandlerError(Exception, Generic[TCrawlingContext]):
    """Wraps an exception thrown from a request handler (router) and extends it with crawling context."""

    def __init__(self, wrapped_exception: Exception, crawling_context: TCrawlingContext) -> None:
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


class ContextPipeline(Generic[TCrawlingContext]):
    def __init__(
        self,
        *,
        _middleware: Callable[
            [TCrawlingContext],
            AsyncGenerator[TMiddlewareCrawlingContext, None],
        ]
        | None = None,
        _parent: ContextPipeline[BasicCrawlingContext] | None = None,
    ) -> None:
        self._middleware = _middleware
        self._parent = _parent

    def _middleware_chain(self) -> Generator[ContextPipeline[Any], None, None]:
        yield self

        if self._parent is not None:
            yield from self._parent._middleware_chain()  # noqa: SLF001

    async def __call__(
        self,
        crawling_context: BasicCrawlingContext,
        final_context_consumer: Callable[[TCrawlingContext], Awaitable[None]],
    ) -> None:
        chain = list(self._middleware_chain())
        cleanup_stack = list[AsyncGenerator]()

        for member in reversed(chain):
            if member._middleware:
                middleware_instance = member._middleware(crawling_context)
                try:
                    result = await middleware_instance.__anext__()
                except StopAsyncIteration as e:
                    raise RuntimeError('The middleware did not yield') from e

                crawling_context = result
                cleanup_stack.append(middleware_instance)

        try:
            await final_context_consumer(cast(TCrawlingContext, crawling_context))
        except Exception as e:
            raise RequestHandlerError(e, crawling_context) from e

        for middleware_instance in reversed(cleanup_stack):
            try:
                result = await middleware_instance.__anext__()
            except StopAsyncIteration:  # noqa: PERF203
                pass
            else:
                raise RuntimeError('The middleware yielded more than once')

    def compose(
        self,
        middleware: Callable[
            [TCrawlingContext],
            AsyncGenerator[TMiddlewareCrawlingContext, None],
        ],
    ) -> ContextPipeline[TMiddlewareCrawlingContext]:
        return ContextPipeline[TMiddlewareCrawlingContext](
            _middleware=cast(
                Callable[[BasicCrawlingContext], AsyncGenerator[TMiddlewareCrawlingContext, None]], middleware
            ),
            _parent=cast(ContextPipeline[BasicCrawlingContext], self),
        )
