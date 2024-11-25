from __future__ import annotations

from typing import Any, Awaitable, Callable, Generic

from typing_extensions import TypeVar

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.basic_crawler._middleware import TInputContext, TOutputContext, Middleware
from crawlee.errors import (
    RequestHandlerError,
    SessionError,
)

TFinalContext = TypeVar('TFinalContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
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
        middleware: Middleware[TInputContext, TOutputContext],
        child: ContextPipeline[TFinalContext, TOutputContext, Any] | None = None, #  Each pipeline knows type of children input, but is not aware of type of children output.
    ) -> None:
        self._middleware = middleware
        self._child = child


    async def __call__(
        self,
        crawling_context: TInputContext,
        final_context_consumer: Callable[[TFinalContext], Awaitable[None]],
    ) -> None:
        """Run a crawling context through the middleware chain and pipe it into a consumer function.

        Exceptions from the consumer function are wrapped together with the final crawling context.
        """
        with self(input_context=crawling_context) as final_context:
            try:
                await final_context_consumer(final_context)
            except SessionError:  # Session errors get special treatment
                raise
            except Exception as e:
                raise RequestHandlerError(e, final_context or crawling_context) from e


    async def __aenter__(self, input_context: TInputContext) -> TFinalContext:
        output_context = await self.middleware.action(input_context)
        if self.child:
            async with self.child(output_context) as child_output_context:
                return child_output_context
        output_context

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.middleware.cleanup()

    def compose(
        self,
        middleware: Middleware[TOutputContext, TChildrenOutputContext]
    ) -> ContextPipeline[TFinalContext, TOutputContext, TChildrenOutputContext]:
        """Add a middleware to the pipeline.

        Returns:
            The extended pipeline instance, providing a fluent interface
        """
        new_pipeline_step = ContextPipeline[TFinalContext, TOutputContext, TChildrenOutputContext](
            middleware=middleware)
        self._child = new_pipeline_step
        return new_pipeline_step
