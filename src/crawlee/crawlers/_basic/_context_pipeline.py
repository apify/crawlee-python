from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Generator
from typing import Any, Callable, Generic, cast

from typing_extensions import TypeVar

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.errors import (
    ContextPipelineFinalizationError,
    ContextPipelineInitializationError,
    ContextPipelineInterruptedError,
    RequestHandlerError,
    SessionError,
)

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TMiddlewareCrawlingContext = TypeVar('TMiddlewareCrawlingContext', bound=BasicCrawlingContext)


@docs_group('Classes')
class ContextPipeline(Generic[TCrawlingContext]):
    """Encapsulates the logic of gradually enhancing the crawling context with additional information and utilities.

    The enhancement is done by a chain of middlewares that are added to the pipeline after it's creation.
    """

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
        """Run a crawling context through the middleware chain and pipe it into a consumer function.

        Exceptions from the consumer function are wrapped together with the final crawling context.
        """
        chain = list(self._middleware_chain())
        cleanup_stack = list[AsyncGenerator]()

        try:
            for member in reversed(chain):
                if member._middleware:  # noqa: SLF001
                    middleware_instance = member._middleware(crawling_context)  # noqa: SLF001
                    try:
                        result = await middleware_instance.__anext__()
                    except SessionError:  # Session errors get special treatment
                        raise
                    except StopAsyncIteration as e:
                        raise RuntimeError('The middleware did not yield') from e
                    except ContextPipelineInterruptedError:
                        raise
                    except Exception as e:
                        raise ContextPipelineInitializationError(e, crawling_context) from e

                    crawling_context = result
                    cleanup_stack.append(middleware_instance)

            try:
                await final_context_consumer(cast('TCrawlingContext', crawling_context))
            except SessionError:  # Session errors get special treatment
                raise
            except Exception as e:
                raise RequestHandlerError(e, crawling_context) from e
        finally:
            for middleware_instance in reversed(cleanup_stack):
                try:
                    result = await middleware_instance.__anext__()
                except StopAsyncIteration:  # noqa: PERF203
                    pass
                except ContextPipelineInterruptedError as e:
                    raise RuntimeError('Invalid state - pipeline interrupted in the finalization step') from e
                except Exception as e:
                    raise ContextPipelineFinalizationError(e, crawling_context) from e
                else:
                    raise RuntimeError('The middleware yielded more than once')

    def compose(
        self,
        middleware: Callable[
            [TCrawlingContext],
            AsyncGenerator[TMiddlewareCrawlingContext, None],
        ],
    ) -> ContextPipeline[TMiddlewareCrawlingContext]:
        """Add a middleware to the pipeline.

        The middleware should yield exactly once, and it should yield an (optionally) extended crawling context object.
        The part before the yield can be used for initialization and the part after it for cleanup.

        Returns:
            The extended pipeline instance, providing a fluent interface
        """
        return ContextPipeline[TMiddlewareCrawlingContext](
            _middleware=cast(
                'Callable[[BasicCrawlingContext], AsyncGenerator[TMiddlewareCrawlingContext, None]]', middleware
            ),
            _parent=cast('ContextPipeline[BasicCrawlingContext]', self),
        )
