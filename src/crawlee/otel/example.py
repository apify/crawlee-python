import asyncio

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import set_tracer_provider

from crawlee._types import BasicCrawlingContext
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.otel.instrumentors.crawler import CrawlerInstrumentor
from crawlee.storages import Dataset, KeyValueStore, RequestQueue

"""
docker run -d --name jaeger2 \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
"""


def instrument_crawler() -> None:
    """Add instrumentation to the crawler."""
    resource = Resource.create(
        {'service.name': 'ExampleCrawler', 'service.version': '1.0.0', 'environment': 'development'}
    )

    # Set up the OpenTelemetry tracer provider and exporter
    provider = TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(endpoint='localhost:4317', insecure=True)
    provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))
    set_tracer_provider(provider)
    # Instrument the crawler with OpenTelemetry
    CrawlerInstrumentor(instrument_classes=[RequestQueue, KeyValueStore, Dataset]).instrument()


async def main() -> None:
    """Run the crawler."""
    instrument_crawler()

    crawler = ParselCrawler(max_requests_per_crawl=10)
    kvs = await KeyValueStore.open()

    @crawler.pre_navigation_hook
    async def pre_nav_hook(_: BasicCrawlingContext) -> None:
        # Simulate some pre-navigation processing
        await asyncio.sleep(0.01)

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})
        await kvs.set_value(key='url', value=context.request.url)
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])


asyncio.run(main())
