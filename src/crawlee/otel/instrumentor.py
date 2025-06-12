# ruff: noqa PGH004

# Monkey patching for OpenTelemetry instrumentation is not type safe, so we disable type checking for this file.
# TODO exclude from type checkers

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.semconv.attributes.code_attributes import CODE_FUNCTION_NAME
from opentelemetry.semconv.attributes.http_attributes import HTTP_REQUEST_METHOD
from opentelemetry.semconv.attributes.url_attributes import URL_FULL
from opentelemetry.trace import get_tracer
from wrapt import wrap_function_wrapper


class CrawlerInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return []

    def _instrument(self, **kwargs):
        tracer = get_tracer(__name__)

        async def middlware_wrapper(wrapped, instance, args, kwargs):
            with tracer.start_as_current_span(
                name = f'{instance._generator.__name__}, {wrapped.__name__}',
                attributes={
                    URL_FULL:instance._context.request.url,
                    CODE_FUNCTION_NAME: instance._generator.__qualname__
                }):
                return await wrapped(*args, **kwargs)


        async def context_pipeline_wrapper(wrapped, instance, args, kwargs):
            context = args[0]
            final_context_consumer = args[1]

            async def wrapped_final_consumer(*args, **kwargs):
                with tracer.start_as_current_span(
                    name = f'request_handler',
                    attributes={
                        URL_FULL:context.request.url,
                        HTTP_REQUEST_METHOD: context.request.method},
                ) as span:
                    return await final_context_consumer(*args, **kwargs)

            with tracer.start_as_current_span(
                name = f'ContextPipeline:{context.request.unique_key}',
                attributes={URL_FULL:context.request.url, HTTP_REQUEST_METHOD: context.request.method}) as span:
                return await wrapped(context, wrapped_final_consumer, **kwargs)

        # Patch request related functionality
        wrap_function_wrapper('crawlee.crawlers._basic._context_pipeline', '_Middleware.action', middlware_wrapper)
        wrap_function_wrapper('crawlee.crawlers._basic._context_pipeline', '_Middleware.cleanup', middlware_wrapper)
        wrap_function_wrapper('crawlee.crawlers._basic._context_pipeline', 'ContextPipeline.__call__', context_pipeline_wrapper)

    def _uninstrument(self, **kwargs):
        raise NotImplementedError("Not yet implemented.")

class AutoscaledPoolInstrumentor():
    def instrumentation_dependencies(self):
        return []

    def _instrument(self, **kwargs):
        raise NotImplementedError("Not yet implemented.")

    def _uninstrument(self, **kwargs):
        raise NotImplementedError("Not yet implemented.")
