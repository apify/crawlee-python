from typing import Awaitable, Callable

from typing_extensions import TypeVar, Generic

from crawlee._types import BasicCrawlingContext
from crawlee.errors import SessionError, ContextPipelineInterruptedError, ContextPipelineInitializationError, \
    ContextPipelineFinalizationError

TInputContext = TypeVar("TInputContext", bound=BasicCrawlingContext, default=BasicCrawlingContext)
TOuputContext = TypeVar("TOuputContext", bound=BasicCrawlingContext, default=BasicCrawlingContext)

class Middleware(Generic[TInputContext, TOuputContext]):
    def __init__(self, action: Callable[[TInputContext], Awaitable[TOuputContext]], cleanup: Callable[[TOuputContext], Awaitable[None]]) -> None:
        self._action=action
        self._cleanup=cleanup
        self._output_context: TOuputContext | None= None
        self._input_context: TInputContext | None = None

    async def action(self, input_context: TInputContext) -> Awaitable[TOuputContext]:
        self._input_context = input_context
        try:
            self._output_context = await self._action(input_context)
        except SessionError:  # Session errors get special treatment
            raise
        except ContextPipelineInterruptedError:
            raise
        except Exception as e:
            raise ContextPipelineInitializationError(e, input_context) from e

    async def cleanup(self) -> Awaitable[None]:
        try:
            await self._cleanup(self._output_context)
        except ContextPipelineInterruptedError as e:
            raise RuntimeError('Invalid state - pipeline interrupted in the finalization step') from e
        except Exception as e:
            raise ContextPipelineFinalizationError(e, self._output_context or self._input_context) from e

