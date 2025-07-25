from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, cast

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

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Generator

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TMiddlewareCrawlingContext = TypeVar('TMiddlewareCrawlingContext', bound=BasicCrawlingContext)


class _Middleware(Generic[TMiddlewareCrawlingContext, TCrawlingContext]):
    """Helper wrapper class to make the middleware easily observable by open telemetry instrumentation."""

    def __init__(
        self,
        middleware: Callable[
            [TCrawlingContext],
            AsyncGenerator[TMiddlewareCrawlingContext, Exception | None],
        ],
        input_context: TCrawlingContext,
    ) -> None:
        self.generator = middleware(input_context)
        self.input_context = input_context
        self.output_context: TMiddlewareCrawlingContext | None = None

    async def action(self) -> TMiddlewareCrawlingContext:
        self.output_context = await self.generator.__anext__()
        return self.output_context

    async def cleanup(self, final_consumer_exception: Exception | None) -> None:
        try:
            await self.generator.asend(final_consumer_exception)
        except StopAsyncIteration:
            pass
        except ContextPipelineInterruptedError as e:
            raise RuntimeError('Invalid state - pipeline interrupted in the finalization step') from e
        except Exception as e:
            raise ContextPipelineFinalizationError(e, self.output_context or self.input_context) from e
        else:
            raise RuntimeError('The middleware yielded more than once')


@docs_group('Other')
class ContextPipeline(Generic[TCrawlingContext]):
    """Encapsulates the logic of gradually enhancing the crawling context with additional information and utilities.

    The enhancement is done by a chain of middlewares that are added to the pipeline after it's creation.
    """

    def __init__(
        self,
        *,
        _middleware: Callable[
            [TCrawlingContext],
            AsyncGenerator[TMiddlewareCrawlingContext, Exception | None],
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
        cleanup_stack: list[_Middleware[Any]] = []
        final_consumer_exception: Exception | None = None

        try:
            for member in reversed(chain):
                if member._middleware:  # noqa: SLF001
                    middleware_instance = _Middleware(middleware=member._middleware, input_context=crawling_context)  # noqa: SLF001
                    try:
                        result = await middleware_instance.action()
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
            except SessionError as e:  # Session errors get special treatment
                final_consumer_exception = e
                raise
            except Exception as e:
                final_consumer_exception = e
                raise RequestHandlerError(e, crawling_context) from e
        finally:
            for middleware_instance in reversed(cleanup_stack):
                await middleware_instance.cleanup(final_consumer_exception)

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
                'Callable[[BasicCrawlingContext], AsyncGenerator[TMiddlewareCrawlingContext, Exception | None]]',
                middleware,
            ),
            _parent=cast('ContextPipeline[BasicCrawlingContext]', self),
        )
