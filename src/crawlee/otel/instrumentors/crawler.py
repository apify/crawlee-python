from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.attributes.code_attributes import CODE_FUNCTION_NAME
from opentelemetry.semconv.attributes.http_attributes import HTTP_REQUEST_METHOD
from opentelemetry.semconv.attributes.url_attributes import URL_FULL
from opentelemetry.trace import get_tracer
from wrapt import wrap_function_wrapper

from crawlee.crawlers import ContextPipeline
from crawlee.crawlers._basic._context_pipeline import _Middleware


class CrawlerInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return ["crawlee"]

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
                ):
                    return await final_context_consumer(*args, **kwargs)

            with tracer.start_as_current_span(
                name = f'ContextPipeline:{context.request.unique_key}',
                attributes={URL_FULL:context.request.url, HTTP_REQUEST_METHOD: context.request.method}) as span:
                return await wrapped(context, wrapped_final_consumer, **kwargs)

        wrap_function_wrapper(_Middleware, 'action', middlware_wrapper)
        wrap_function_wrapper(_Middleware, 'cleanup', middlware_wrapper)
        wrap_function_wrapper(ContextPipeline, '__call__', context_pipeline_wrapper)

    def _uninstrument(self, **kwargs):
        unwrap(ContextPipeline, '__call__')
        unwrap(_Middleware, 'action')
        unwrap(_Middleware, 'cleanup')
