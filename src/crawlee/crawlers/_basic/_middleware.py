from typing import Awaitable, Callable, AsyncGenerator

from typing_extensions import TypeVar, Generic

from crawlee._types import BasicCrawlingContext
from crawlee.errors import ContextPipelineInitializationError, ContextPipelineInterruptedError, SessionError, \
    ContextPipelineFinalizationError

TInputContext = TypeVar("TInputContext", bound=BasicCrawlingContext, default=BasicCrawlingContext)
TOutputContext = TypeVar("TOutputContext", bound=BasicCrawlingContext, default=BasicCrawlingContext)

class Middleware(Generic[TInputContext, TOutputContext]):
    def __init__(self,
                 middleware_initializer = Callable[[TInputContext], AsyncGenerator[TOutputContext, None]],
                 ) -> None:

        self._middleware_initializer= middleware_initializer
        self._middleware_generator: AsyncGenerator[TOutputContext, None] = None
        self._output_context: TOutputContext | None= None
        self._input_context: TInputContext | None = None

    async def action(self, input_context: TInputContext) -> Awaitable[TOutputContext]:
        self._input_context = input_context
        self._middleware_generator = self._middleware_initializer(self._input_context)
        try:
            self._output_context = await self._middleware_generator.__anext__()
            return self._output_context
        except StopAsyncIteration as e:
            raise RuntimeError('The middleware did not yield') from e
        except SessionError:  # Session errors get special treatment
            raise
        except ContextPipelineInterruptedError:
            raise
        except Exception as e:
            raise ContextPipelineInitializationError(e, input_context) from e

    async def cleanup(self, exception: Exception) -> Awaitable[None]:
        try:
            await self._middleware_generator.asend(exception)
        except StopAsyncIteration:  # noqa: PERF203
            # Expected exception. self._middleware_generator should yield exactly once. Cleanup code does not yield.
            pass
        except ContextPipelineInterruptedError as e:
            raise RuntimeError('Invalid state - pipeline interrupted in the finalization step') from e
        except Exception as e:
            raise ContextPipelineFinalizationError(e, self._output_context or self._input_context) from e
        else:
            raise RuntimeError('The middleware yielded more than once')

    @classmethod
    def create_no_action_middleware(cls):
        async def no_action_middleware(crawling_context: BasicCrawlingContext) ->AsyncGenerator[BasicCrawlingContext, None]:
            yield crawling_context
        return cls[BasicCrawlingContext,BasicCrawlingContext](middleware_initializer=no_action_middleware)
