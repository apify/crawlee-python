import io
import json
import re
from unittest import mock

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import set_tracer_provider
from yarl import URL

from crawlee.crawlers import ParselCrawler
from crawlee.otel.crawler_instrumentor import CrawlerInstrumentor
from crawlee.storages import Dataset


async def test_crawler_instrumentor_capability(server_url: URL) -> None:
    """Test OpenTelemetry instrumentation capability of the crawler.

    Instrument the crawler and one additional class and check that telemetry data is generated correctly.
    Telemetry data is redirected to an in-memory file for testing purposes."""

    resource = Resource.create(
        {
            'service.name': 'ExampleCrawler',
            'service.version': '1.0.0',
            'environment': 'development',
        }
    )
    # Set up the OpenTelemetry tracer provider and exporter
    provider = TracerProvider(resource=resource)
    in_memory_sink_for_telemetry = io.StringIO(newline='\n')
    exporter = ConsoleSpanExporter(out=in_memory_sink_for_telemetry)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    set_tracer_provider(provider)
    # Instrument the crawler with OpenTelemetry
    CrawlerInstrumentor(instrument_classes=[Dataset]).instrument()

    # Generate first telemetry data from `Dataset` public methods.
    # `Dataset` is in `instrument_classes` argument, and thus it's public methods are instrumented.
    dataset = await Dataset.open(name='test_dataset')
    await dataset.drop()

    # Other traces will be from crawler run.
    crawler = ParselCrawler(max_requests_per_crawl=1, request_handler=mock.AsyncMock())

    # Run crawler and generate more telemetry data.
    await crawler.run([str(server_url)])

    # Telemetry jsons are packed together in one string. Unpack them and load as json objects.
    telemetry_strings = in_memory_sink_for_telemetry.getvalue()
    telemetry_data = [
        json.loads(telemetry_string) for telemetry_string in re.split(r'(?<=\})\s*(?=\{)', telemetry_strings)
    ]

    # Do some basic checks on the telemetry data.
    # The point of this test is not to check completeness of the data, but telemetry capability.

    # Extra `instrument_classes` telemetry - KeyValueStore.open() is parent to KeyValueStore.__init__() span.
    assert telemetry_data[0]['name'] == '__init__'
    assert telemetry_data[0]['attributes']['code.function.name'] == 'Dataset.__init__'
    assert telemetry_data[0]['resource']['attributes'] == dict(resource.attributes)

    assert telemetry_data[1]['name'] == 'open'
    assert telemetry_data[1]['attributes']['code.function.name'] == 'Dataset.open'
    assert telemetry_data[1]['resource']['attributes'] == dict(resource.attributes)

    # Opening KeyValueStore creates a new trace.
    assert telemetry_data[0]['context']['trace_id'] == telemetry_data[1]['context']['trace_id']

    assert telemetry_data[2]['name'] == 'drop'
    assert telemetry_data[2]['attributes']['code.function.name'] == 'Dataset.drop'
    assert telemetry_data[2]['resource']['attributes'] == dict(resource.attributes)

    # Dropping KeyValueStore creates a new trace.
    assert telemetry_data[2]['context']['trace_id'] != telemetry_data[1]['context']['trace_id']

    # Crawler telemetry - all crawler spans will be in one trace as there is only one request in this test.
    assert telemetry_data[3]['name'] == '_execute_pre_navigation_hooks, action'
    assert telemetry_data[3]['attributes']['code.function.name'] == 'AbstractHttpCrawler._execute_pre_navigation_hooks'
    assert telemetry_data[3]['attributes']['url.full'] == str(server_url)
    assert telemetry_data[3]['resource']['attributes'] == dict(resource.attributes)

    assert telemetry_data[-1]['name'] == '__run_task_function'
    assert telemetry_data[-1]['attributes']['code.function.name'] == 'BasicCrawler.__run_task_function'
    assert telemetry_data[-1]['resource']['attributes'] == dict(resource.attributes)

    # Processing of the request is in the same trace.
    assert telemetry_data[3]['context']['trace_id'] == telemetry_data[-1]['context']['trace_id']

    # Check that trace_ids of unrelated traces are not the same.
    assert telemetry_data[0]['context']['trace_id'] != telemetry_data[-1]['context']['trace_id']
