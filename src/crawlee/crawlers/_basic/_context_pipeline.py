from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Generic, cast

from typing_extensions import TypeVar, Self

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from ._middleware import TInputContext, TOutputContext, Middleware
from crawlee.errors import ContextPipelineInitializationError, ContextPipelineInterruptedError, SessionError, \
    RequestHandlerError, ContextPipelineFinalizationError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Generator

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TMiddlewareCrawlingContext = TypeVar('TMiddlewareCrawlingContext', bound=BasicCrawlingContext)

TFinalContext = TypeVar("TFinalContext", bound=BasicCrawlingContext, default=BasicCrawlingContext)
TChildInputContext = TypeVar('TChildInputContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TChildOutputContext = TypeVar('TChildOutputContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)



@docs_group('Classes')
class ContextPipeline(Generic[TFinalContext]):
    """Encapsulates the logic of gradually enhancing the crawling context with additional information and utilities.

    The enhancement is done by a chain of middlewares that are added to the pipeline after it's creation.
    """

    def __init__(
        self,
        middleware: Callable[[TInputContext], AsyncGenerator[TOutputContext, None]] | None = None
    ):
        if middleware is not None:
            self._first_step = _ContextPipelineStep(Middleware(middleware))
        else:
            self._first_step = _ContextPipelineStep(Middleware.create_no_action_middleware())

    def compose(self, middleware: Callable[[TInputContext], AsyncGenerator[TOutputContext, None]])->Self:
        self._first_step.add_last_step(middleware=Middleware(middleware))
        return self

    async def __call__(
        self,
        crawling_context: BasicCrawlingContext,
        final_context_consumer: Callable[[TFinalContext], Awaitable[None]],
    ) -> None:
        await self._first_step.execute_chain(crawling_context=crawling_context, final_context_consumer=final_context_consumer)

class _ContextPipelineStep(Generic[TInputContext, TOutputContext]):
    """A single step in the context pipeline.

    It can hold reference to the next step in the pipeline, if not it is the last step in the pipeline.
    """

    def __init__(self, middleware: Middleware[TInputContext, TOutputContext]) -> None:
        self._middleware =middleware
        self.child: _ContextPipelineStep[TOutputContext, Any] | None= None

    async def execute_chain(self, crawling_context: TInputContext, final_context_consumer: TFinalContext):
        _exception: Exception | None = None
        output_context = await self._middleware.action(crawling_context)
        try:
            if self.child:
                await self.child.execute_chain(crawling_context=output_context,
                                               final_context_consumer=final_context_consumer)
            else:
                try:
                    await final_context_consumer(output_context)
                except SessionError as e:  # Session errors get special treatment
                    final_consumer_exception = e
                    raise
                except Exception as e:
                    final_consumer_exception = e
                    raise RequestHandlerError(e, crawling_context) from e

        except Exception as e:
            _exception = e
            raise
        finally:
            await self._middleware.cleanup(_exception)


    def _add_next_step(self, middleware: Middleware[TOutputContext, TChildOutputContext]
                       )-> _ContextPipelineStep[TOutputContext, TChildOutputContext]:
        """Add new step directly after this step."""
        new_pipeline_step = _ContextPipelineStep[TOutputContext, TChildOutputContext](middleware=middleware)
        self.child = new_pipeline_step
        return new_pipeline_step

    def add_last_step(self, middleware: Middleware[TChildInputContext, TChildOutputContext]) -> None:
        """Add step to the last step in the pipeline."""
        if not self.child:
            self.child = self._add_next_step(middleware=middleware)
        else:
            self.child.add_last_step(middleware=middleware)
        return self

