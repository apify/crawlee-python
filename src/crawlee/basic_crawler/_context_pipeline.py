from __future__ import annotations

from contextlib import asynccontextmanager, ExitStack
from typing import Any, Awaitable, Callable, Generic, AsyncGenerator

from typing_extensions import TypeVar

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.basic_crawler._middleware import TInputContext, TOutputContext, Middleware
from crawlee.errors import (
    RequestHandlerError,
    SessionError,
)
TFinalContext = TypeVar("TFinalContext", bound=BasicCrawlingContext, default=BasicCrawlingContext)

TChildrenOutputContext = TypeVar('TChildrenOutputContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)

@docs_group('Classes')
class ContextPipeline(Generic[TFinalContext, TInputContext, TOutputContext]):
    """Encapsulates the logic of gradually enhancing the crawling context with additional information and utilities.

    The enhancement is done by a chain of middlewares that are added to the pipeline after its creation.
    Each pipeline instance in chain works with 3 different contexts:
        TInputContext is context entering this pipeline and is input to its middleware.
        TOutputContext is context produced by running its middleware and will be input to its child middleware.
        TFinalContext is context that must be produced by the last ContextPipeline's middleware.
        First ContextPipeline will have TInputContext=BasicCrawlingContext
        Last ContextPipeline will have TOutputContext=TFinalContext
    """

    def __init__(
        self,
        *,
        middleware: Callable[[TInputContext], AsyncGenerator[TOutputContext, None]] | None = None,
        child: ContextPipeline[TFinalContext, TOutputContext, Any] | None = None, #  Each pipeline knows type of children input, but is not aware of type of children output.
    ) -> None:
        if middleware is not None:
            self._middleware = Middleware(middleware_factory=middleware)
        else:
            self._middleware = Middleware.create_empty_middleware()
        self.child = child


    async def __call__(
        self,
        crawling_context: TInputContext,
        final_context_consumer: Callable[[TFinalContext], Awaitable[None]],
    ) -> None:
        """Run a crawling context through the middleware chain and pipe it into a consumer function.

        Exceptions from the consumer function are wrapped together with the final crawling context.
        """

        await _MiddlewareContext(
            input_context=crawling_context,
            final_context_consumer = final_context_consumer,
            context_pipeline =self).execute_in_final_context()


    async def execute_in_final_context(self, crawling_context: TInputContext,
                                       final_context_consumer: Callable[[TFinalContext], Awaitable[None]],) -> TFinalContext:
        output_context = await self._middleware.action(crawling_context)
        if self.child:
            self.child.input_context = output_context
            return await self.child.get_final_context(output_context)
        return output_context

    def compose(
        self,
        middleware: Callable[[TInputContext], AsyncGenerator[TOutputContext, None]]
    ) -> ContextPipeline[TFinalContext, TOutputContext, TChildrenOutputContext]:
        """Add a middleware to the pipeline.

        Returns:
            The extended pipeline instance, providing a fluent interface
        """
        new_pipeline_step = ContextPipeline[TFinalContext, TOutputContext, TChildrenOutputContext](
            middleware=middleware)
        self.child = new_pipeline_step
        return new_pipeline_step


class _MiddlewareContext():
    def __init__(self, input_context: TInputContext,  final_context_consumer, context_pipeline):
        self._input_context = input_context
        self._final_context_consumer = final_context_consumer
        self._context_pipeline = context_pipeline

    async def __aenter__(self) -> TOutputContext:
        output_context = await self._context_pipeline._middleware.action(self._input_context)
        return output_context


    async def execute_in_final_context(self) -> None:
        async with self as output_context:
            if self._context_pipeline.child is not None:
                await _MiddlewareContext(output_context, self._final_context_consumer,
                                                            self._context_pipeline.child).execute_in_final_context()
            else:
                try:
                    await self._final_context_consumer(output_context)
                except SessionError:  # Session errors get special treatment
                    raise
                except Exception as e:
                    raise RequestHandlerError(e, output_context or self._input_context) from e


    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._context_pipeline._middleware.cleanup()

