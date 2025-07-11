from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from opentelemetry.instrumentation.instrumentor import (  # type:ignore[attr-defined]  # Mypy has troubles with OTEL
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.attributes.code_attributes import CODE_FUNCTION_NAME
from opentelemetry.semconv.attributes.http_attributes import HTTP_REQUEST_METHOD
from opentelemetry.semconv.attributes.url_attributes import URL_FULL
from opentelemetry.trace import get_tracer
from wrapt import wrap_function_wrapper

from crawlee._utils.docs import docs_group
from crawlee.crawlers import BasicCrawler, ContextPipeline
from crawlee.crawlers._basic._context_pipeline import _Middleware

if TYPE_CHECKING:
    from collections.abc import Callable

    from crawlee.crawlers import BasicCrawlingContext


@docs_group('Classes')
class CrawlerInstrumentor(BaseInstrumentor):
    """Helper class for instrumenting crawlers with OpenTelemetry."""

    def __init__(
        self, *, instrument_classes: list[type] | None = None, request_handling_instrumentation: bool = True
    ) -> None:
        """Initialize the instrumentor.

        Args:
            instrument_classes: List of classes to be instrumented - all their public methods and coroutines will be
                wrapped by generic instrumentation wrapper that will create spans for them.
            request_handling_instrumentation: Handpicked most interesting methods to instrument in the request handling
                pipeline.
        """
        self._tracer = get_tracer(__name__)

        async def _simple_async_wrapper(wrapped: Any, _: Any, args: Any, kwargs: Any) -> Any:
            with self._tracer.start_as_current_span(
                name=wrapped.__name__, attributes={CODE_FUNCTION_NAME: wrapped.__qualname__}
            ):
                return await wrapped(*args, **kwargs)

        def _simple_wrapper(wrapped: Any, _: Any, args: Any, kwargs: Any) -> Any:
            with self._tracer.start_as_current_span(
                name=wrapped.__name__, attributes={CODE_FUNCTION_NAME: wrapped.__qualname__}
            ):
                return wrapped(*args, **kwargs)

        def _init_wrapper(wrapped: Any, _: Any, args: Any, kwargs: Any) -> None:
            with self._tracer.start_as_current_span(
                name=wrapped.__name__, attributes={CODE_FUNCTION_NAME: wrapped.__qualname__}
            ):
                wrapped(*args, **kwargs)

        self._instrumented: list[tuple[Any, str, Callable]] = []
        self._simple_wrapper = _simple_wrapper
        self._simple_async_wrapper = _simple_async_wrapper
        self._init_wrapper = _init_wrapper

        if instrument_classes:
            for _class in instrument_classes:
                self._instrument_all_public_methods(on_class=_class)

        if request_handling_instrumentation:

            async def middlware_wrapper(wrapped: Any, instance: _Middleware, args: Any, kwargs: Any) -> Any:
                with self._tracer.start_as_current_span(
                    name=f'{instance.generator.__name__}, {wrapped.__name__}',  # type:ignore[attr-defined]  # valid in our context
                    attributes={
                        URL_FULL: instance.input_context.request.url,
                        CODE_FUNCTION_NAME: instance.generator.__qualname__,  # type:ignore[attr-defined]  # valid in our context
                    },
                ):
                    return await wrapped(*args, **kwargs)

            async def context_pipeline_wrapper(
                wrapped: Any, _: ContextPipeline[BasicCrawlingContext], args: Any, kwargs: Any
            ) -> Any:
                context = args[0]
                final_context_consumer = args[1]

                async def wrapped_final_consumer(*args: Any, **kwargs: Any) -> Any:
                    with self._tracer.start_as_current_span(
                        name='request_handler',
                        attributes={URL_FULL: context.request.url, HTTP_REQUEST_METHOD: context.request.method},
                    ):
                        return await final_context_consumer(*args, **kwargs)

                with self._tracer.start_as_current_span(
                    name='ContextPipeline',
                    attributes={URL_FULL: context.request.url, HTTP_REQUEST_METHOD: context.request.method},
                ):
                    return await wrapped(context, wrapped_final_consumer, **kwargs)

            async def _commit_request_handler_result_wrapper(
                wrapped: Callable[[Any], Any], _: BasicCrawler, args: Any, kwargs: Any
            ) -> Any:
                context = args[0]
                with self._tracer.start_as_current_span(
                    name='Commit results',
                    attributes={URL_FULL: context.request.url, HTTP_REQUEST_METHOD: context.request.method},
                ):
                    return await wrapped(*args, **kwargs)

            # Handpicked interesting methods to instrument
            self._instrumented.extend(
                [
                    (_Middleware, 'action', middlware_wrapper),
                    (_Middleware, 'cleanup', middlware_wrapper),
                    (ContextPipeline, '__call__', context_pipeline_wrapper),
                    (BasicCrawler, '_BasicCrawler__run_task_function', self._simple_async_wrapper),
                    (BasicCrawler, '_commit_request_handler_result', _commit_request_handler_result_wrapper),
                ]
            )

    def instrumentation_dependencies(self) -> list[str]:
        """Return a list of python packages with versions that will be instrumented."""
        return ['crawlee']

    def _instrument_all_public_methods(self, on_class: type) -> None:
        public_coroutines = {
            name
            for name, member in inspect.getmembers(on_class, predicate=inspect.iscoroutinefunction)
            if not name.startswith('_')
        }
        public_methods = {
            name
            for name, member in inspect.getmembers(on_class, predicate=inspect.isfunction)
            if not name.startswith('_')
        } - public_coroutines

        for coroutine in public_coroutines:
            self._instrumented.append((on_class, coroutine, self._simple_async_wrapper))

        for method in public_methods:
            self._instrumented.append((on_class, method, self._simple_wrapper))

        self._instrumented.append((on_class, '__init__', self._init_wrapper))

    def _instrument(self, **_: Any) -> None:
        for _class, method, wrapper in self._instrumented:
            wrap_function_wrapper(_class, method, wrapper)

    def _uninstrument(self, **_: Any) -> None:
        for _class, method, __ in self._instrumented:
            unwrap(_class, method)
