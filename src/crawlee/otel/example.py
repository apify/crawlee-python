import asyncio
import time

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import set_tracer_provider

from crawlee._types import BasicCrawlingContext
from crawlee.crawlers import ParselCrawler
from crawlee.otel.instrumentors.crawler import CrawlerInstrumentor

"""
docker run -d --name jaeger2 \
  -e COLLECTOR_OTLP_ENABLED=true \
  -e COLLECTOR_OTLP_METRICS_ENABLED=true \
  -e METRICS_STORAGE_TYPE=prometheus \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
"""

def instrument_crawler():
    resource = Resource.create({'service.name': 'ParselCrawler',     'service.version': '1.0.0',
    'environment': 'development'})

    # Traces
    provider = TracerProvider(resource=resource)
    exporter = ConsoleSpanExporter()
    otlp_exporter = OTLPSpanExporter(endpoint='localhost:4317', insecure=True)
    #provider.add_span_processor(SimpleSpanProcessor(exporter))
    provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))
    # Set the global tracer provider
    set_tracer_provider(provider)
    CrawlerInstrumentor().instrument()


async def main():
    instrument_crawler()

    crawler = ParselCrawler()

    @crawler.pre_navigation_hook
    async def pre_nav_hook(context: BasicCrawlingContext) -> None:
        time.sleep(0.01)

    @crawler.router.default_handler
    async def handler(context: BasicCrawlingContext) -> None:
        time.sleep(0.2)

    await crawler.run(['https://crawlee.dev/', 'https://www.example.com', 'https://httpbin.org/'])



asyncio.run(main())
